import asyncio
import logging

import pytest

from grctl.models import (
    ErrorDetails,
    HistoryKind,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunTerminated,
    RunTimeout,
)
from grctl.models.errors import WorkflowError
from grctl.workflow.future import WorkflowFuture
from tests.unit.workflow.fakes import (
    DEFAULT_RUN_INFO,
    LOGGER,
    CapturingListenerFactory,
    FakeResultDecoder,
    make_history_event,
)


def make_future(
    return_type: type | None = None,
    decoder: FakeResultDecoder | None = None,
) -> tuple[WorkflowFuture, CapturingListenerFactory]:
    factory = CapturingListenerFactory()
    future = WorkflowFuture(DEFAULT_RUN_INFO, factory, LOGGER, return_type=return_type, decoder=decoder)
    return future, factory


async def test_start_and_stop_delegate_to_the_listener() -> None:
    future, factory = make_future()

    await future.start()
    assert factory.listener.start_calls == 1

    await future.stop()
    assert factory.listener.stop_calls == 1
    assert future.cancelled()


async def test_stop_does_not_cancel_an_already_done_future() -> None:
    future, factory = make_future()
    future.set_result("done")

    await future.stop()

    assert factory.listener.stop_calls == 1
    assert future.result() == "done"


async def test_run_completed_event_sets_the_result() -> None:
    future, factory = make_future()

    factory.deliver(make_history_event(HistoryKind.run_completed, RunCompleted(result=42, duration_ms=10)))

    assert future.done()
    assert await future == 42


async def test_run_completed_decodes_result_when_return_type_and_decoder_are_set() -> None:
    decoder = FakeResultDecoder(decoded="decoded-value")
    future, factory = make_future(return_type=str, decoder=decoder)

    factory.deliver(make_history_event(HistoryKind.run_completed, RunCompleted(result=42, duration_ms=10)))

    assert await future == "decoded-value"
    assert decoder.calls == [(42, str)]


async def test_run_failed_event_sets_a_workflow_error() -> None:
    future, factory = make_future()

    factory.deliver(
        make_history_event(
            HistoryKind.run_failed,
            RunFailed(error=ErrorDetails(type="ValueError", message="bad input", stack_trace=""), duration_ms=10),
        )
    )

    assert future.done()
    with pytest.raises(WorkflowError, match="ValueError: bad input"):
        await future


async def test_run_timeout_event_sets_a_timeout_error() -> None:
    future, factory = make_future()

    factory.deliver(make_history_event(HistoryKind.run_timeout, RunTimeout(duration_ms=5000)))

    with pytest.raises(TimeoutError):
        await future


async def test_run_cancelled_event_sets_a_cancelled_error() -> None:
    future, factory = make_future()

    factory.deliver(make_history_event(HistoryKind.run_cancelled, RunCancelled(reason="user", duration_ms=10)))

    with pytest.raises(asyncio.CancelledError):
        await future


async def test_run_terminated_event_sets_a_cancelled_error() -> None:
    future, factory = make_future()

    factory.deliver(make_history_event(HistoryKind.run_terminated, RunTerminated(reason="force", duration_ms=10)))

    with pytest.raises(asyncio.CancelledError):
        await future


@pytest.mark.parametrize("kind", [HistoryKind.run_scheduled, HistoryKind.run_started])
async def test_non_terminal_events_do_not_complete_the_future(kind: HistoryKind) -> None:
    future, factory = make_future()

    factory.deliver(make_history_event(kind, RunCompleted(result=None, duration_ms=0)))

    assert not future.done()


async def test_unknown_event_kind_is_ignored_without_raising() -> None:
    future, factory = make_future()

    factory.deliver(make_history_event(HistoryKind.step_started, RunCompleted(result=None, duration_ms=0)))

    assert not future.done()


async def test_terminal_event_delivered_twice_is_a_noop_the_second_time() -> None:
    future, factory = make_future()

    factory.deliver(make_history_event(HistoryKind.run_completed, RunCompleted(result=1, duration_ms=10)))
    factory.deliver(make_history_event(HistoryKind.run_completed, RunCompleted(result=2, duration_ms=10)))

    assert await future == 1


async def test_mismatched_payload_type_is_logged_and_does_not_raise() -> None:
    future, factory = make_future()

    factory.deliver(
        make_history_event(
            HistoryKind.run_completed,
            RunFailed(error=ErrorDetails(type="X", message="y", stack_trace=""), duration_ms=1),
        )
    )

    assert not future.done()


async def test_handler_exception_sets_the_future_exception() -> None:
    future, factory = make_future()

    class ExplodingLogger(logging.Logger):
        def debug(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("boom")

    future._logger = ExplodingLogger("boom")

    factory.deliver(make_history_event(HistoryKind.run_scheduled, RunCompleted(result=None, duration_ms=0)))

    assert future.done()
    with pytest.raises(RuntimeError, match="boom"):
        await future


async def test_discard_stops_listener_and_retrieves_a_pending_exception() -> None:
    future, factory = make_future()
    future.set_exception(ValueError("unretrieved"))

    await future.discard()

    assert factory.listener.stop_calls == 1
    # exception() called internally to mark it retrieved; asyncio should not warn.


async def test_schedule_listener_stop_runs_on_completion() -> None:
    future, factory = make_future()

    future.set_result("done")
    # The done callback is scheduled via call_soon, and it in turn schedules the
    # stop() coroutine as its own task — two ticks of the loop are needed for it to run.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert factory.listener.stop_calls == 1
