"""ACK/NAK semantics of the worker task subscriber, against a real broker.

Every case here is a durability guarantee: whether a message comes back after a
failure decides between a lost run and a duplicated one. Redelivery is observed
through the broker rather than by asserting on `msg.ack()` calls, so these tests
fail when the guarantee breaks — not when the implementation is rearranged.
"""

import asyncio
import logging

import pytest

from grctl.models import Directive
from grctl.models.errors import WorkflowStepAlreadyExecutedError

from .conftest import ACK_WAIT_SECONDS, REDELIVERY_GRACE_SECONDS, make_directive

pytestmark = pytest.mark.usefixtures("fast_ack_settings")


async def _completes() -> None:
    """Finish normally, as a healthy step would."""


async def _raises() -> None:
    raise RuntimeError("step failed")


async def _sleeps_past_ack_wait() -> None:
    await asyncio.sleep(ACK_WAIT_SECONDS * 2)


async def _sleeps_indefinitely() -> None:
    await asyncio.sleep(3600)


class DeliveryBarrier:
    """Fires once a number of deliveries has landed, across however many recorders."""

    def __init__(self, target: int) -> None:
        self._target = target
        self._count = 0
        self.reached = asyncio.Event()

    def record(self) -> None:
        self._count += 1
        if self._count >= self._target:
            self.reached.set()


class DirectiveRecorder:
    """Stands in for ExecutionManager.handle_next_directive.

    Records every delivery and returns the task the subscriber awaits, so a test
    can choose how the run behaves: completing, raising, hanging, or failing to
    start at all.
    """

    def __init__(
        self,
        task_body=_completes,
        init_error: Exception | None = None,
        barrier: DeliveryBarrier | None = None,
    ) -> None:
        self.directives: list[Directive] = []
        self.tasks: list[asyncio.Task] = []
        self.delivered = asyncio.Event()
        self.redelivered = asyncio.Event()
        self._task_body = task_body
        self._init_error = init_error
        self._barrier = barrier

    async def __call__(self, directive: Directive) -> asyncio.Task:
        self.directives.append(directive)
        self.delivered.set()
        if len(self.directives) >= 2:
            self.redelivered.set()
        if self._barrier is not None:
            self._barrier.record()

        if self._init_error is not None:
            raise self._init_error

        task = asyncio.create_task(self._task_body())
        self.tasks.append(task)
        return task

    @property
    def delivery_count(self) -> int:
        return len(self.directives)


class LogPhraseCounter(logging.Handler):
    """Counts log records containing a phrase, for paths with no other observable.

    A message that fails to decode never reaches the directive handler, so the
    subscriber's own log is the only place its redelivery shows up.
    """

    def __init__(self, phrase: str) -> None:
        super().__init__()
        self._phrase = phrase
        self.count = 0
        self.seen_twice = asyncio.Event()

    def emit(self, record: logging.LogRecord) -> None:
        if self._phrase in record.getMessage():
            self.count += 1
            if self.count >= 2:
                self.seen_twice.set()


async def test_completed_run_is_acked_and_not_redelivered(unique_wf_type, publish_directive, start_subscriber) -> None:
    recorder = DirectiveRecorder()
    await start_subscriber(unique_wf_type, recorder)

    await publish_directive(make_directive(unique_wf_type))
    await asyncio.wait_for(recorder.delivered.wait(), timeout=10.0)
    await asyncio.sleep(REDELIVERY_GRACE_SECONDS)

    assert recorder.delivery_count == 1


async def test_initialisation_failure_naks_and_redelivers(unique_wf_type, publish_directive, start_subscriber) -> None:
    """A run that cannot be started must come back — another worker may serve it."""
    recorder = DirectiveRecorder(init_error=ValueError("No workflow registered"))
    await start_subscriber(unique_wf_type, recorder)

    await publish_directive(make_directive(unique_wf_type))
    await asyncio.wait_for(recorder.redelivered.wait(), timeout=15.0)

    assert recorder.delivery_count >= 2


async def test_duplicate_run_is_acked_without_redelivery(unique_wf_type, publish_directive, start_subscriber) -> None:
    """A directive for a run already in flight is a duplicate, not a failure."""
    recorder = DirectiveRecorder(init_error=WorkflowStepAlreadyExecutedError("already running"))
    await start_subscriber(unique_wf_type, recorder)

    await publish_directive(make_directive(unique_wf_type))
    await asyncio.wait_for(recorder.delivered.wait(), timeout=10.0)
    await asyncio.sleep(REDELIVERY_GRACE_SECONDS)

    assert recorder.delivery_count == 1


async def test_failed_run_is_acked_without_redelivery(unique_wf_type, publish_directive, start_subscriber) -> None:
    """A run that started and failed must not come back.

    The failure is already recorded in history, so redelivering would re-run work
    that is known to be done.
    """
    recorder = DirectiveRecorder(task_body=_raises)
    await start_subscriber(unique_wf_type, recorder)

    await publish_directive(make_directive(unique_wf_type))
    await asyncio.wait_for(recorder.delivered.wait(), timeout=10.0)
    await asyncio.sleep(REDELIVERY_GRACE_SECONDS)

    assert recorder.delivery_count == 1


async def test_cancelled_run_is_acked_without_redelivery(unique_wf_type, publish_directive, start_subscriber) -> None:
    recorder = DirectiveRecorder(task_body=_sleeps_indefinitely)
    await start_subscriber(unique_wf_type, recorder)

    await publish_directive(make_directive(unique_wf_type))
    await asyncio.wait_for(recorder.delivered.wait(), timeout=10.0)

    recorder.tasks[0].cancel()
    await asyncio.sleep(REDELIVERY_GRACE_SECONDS)

    assert recorder.delivery_count == 1


async def test_long_running_run_is_not_redelivered(unique_wf_type, publish_directive, start_subscriber) -> None:
    """The in-progress heartbeat must hold off ack_wait for a slow step.

    Without it, any step outliving ack_wait would be redelivered and run twice.
    """
    recorder = DirectiveRecorder(task_body=_sleeps_past_ack_wait)
    await start_subscriber(unique_wf_type, recorder)

    await publish_directive(make_directive(unique_wf_type))
    await asyncio.wait_for(recorder.delivered.wait(), timeout=10.0)
    await asyncio.sleep(ACK_WAIT_SECONDS * 2 + REDELIVERY_GRACE_SECONDS)

    assert recorder.delivery_count == 1


async def test_undecodable_message_is_naked_and_redelivered(
    unique_wf_type, publish_raw, start_subscriber, subscriber_logger
) -> None:
    counter = LogPhraseCounter("Failed to decode directive")
    subscriber_logger.addHandler(counter)
    recorder = DirectiveRecorder()

    try:
        await start_subscriber(unique_wf_type, recorder)
        await publish_raw(unique_wf_type, b"not a directive")
        await asyncio.wait_for(counter.seen_twice.wait(), timeout=15.0)
    finally:
        subscriber_logger.removeHandler(counter)

    assert counter.count >= 2
    assert recorder.delivery_count == 0


async def test_queue_group_distributes_across_workers(unique_wf_type, publish_directive, start_subscriber) -> None:
    """Two workers on one wf_type share the messages and never duplicate them."""
    barrier = DeliveryBarrier(target=2)
    first = DirectiveRecorder(barrier=barrier)
    second = DirectiveRecorder(barrier=barrier)
    await start_subscriber(unique_wf_type, first)
    await start_subscriber(unique_wf_type, second)

    await publish_directive(make_directive(unique_wf_type))
    await publish_directive(make_directive(unique_wf_type))

    await asyncio.wait_for(barrier.reached.wait(), timeout=15.0)
    await asyncio.sleep(REDELIVERY_GRACE_SECONDS)

    handled = [directive.id for directive in first.directives + second.directives]
    assert len(handled) == 2
    assert len(set(handled)) == 2
