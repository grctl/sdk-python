"""Shared fakes and the record/replay harness for exec unit tests.

The one invariant every operation must satisfy is: replaying recorded history
reproduces the same value without redoing side effects. `record_then_replay`
runs a call twice — once against fresh history, once against a journal seeded
with what the first call recorded — and asserts that invariant so individual
tests only need to assert what's specific to the operation under test.
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.context import Context, Store
from grctl.exec.drc_factory import DrcFactory
from grctl.exec.journal import Journal
from grctl.exec.kv_manager import KVManager
from grctl.exec.manager import Codec
from grctl.exec.step_history import HistoryCreateInput
from grctl.exec.workflow_logger import build_workflow_logger
from grctl.models import Directive, DirectiveKind, HistoryEvent, RunInfo, Step
from grctl.nats.codec import MsgspecCodec
from grctl.workflow.handle import WorkflowHandleFactory
from grctl.workflow.workflow import StepInfo

DEFAULT_RUN_INFO = RunInfo(id="run-1", wf_id="wf-1", wf_type="test-workflow")
DEFAULT_WORKER_ID = "worker-1"


class FakeAppender:
    """In-memory stand-in for StepHistory: stamps identity and keeps a durable log."""

    def __init__(self) -> None:
        self.events: list[HistoryEvent] = []

    async def append(self, entry: HistoryCreateInput) -> None:
        self.events.append(
            HistoryEvent(
                wf_id=DEFAULT_RUN_INFO.wf_id,
                run_id=DEFAULT_RUN_INFO.id,
                worker_id=DEFAULT_WORKER_ID,
                timestamp=entry.timestamp,
                kind=entry.kind,
                msg=entry.payload,
                operation_id=entry.operation_id,
            )
        )


class FakeWorkflowAPI:
    """Records each run-scoped call as intent instead of putting a command on the wire."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, RunInfo]] = []

    async def start_run(self, run_info: RunInfo, input: Any, sender_id: str) -> None:  # noqa: A002
        self.calls.append(("start_run", run_info))

    async def send_event(self, run_info: RunInfo, event_name: str, payload: Any, sender_id: str) -> None:
        self.calls.append(("send_event", run_info))

    async def cancel_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None:
        self.calls.append(("cancel_run", run_info))

    async def terminate_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None:
        self.calls.append(("terminate_run", run_info))


class FakeHistoryListener:
    """Stand-in for a HistoryListener: nothing delivers events, but lifecycle is recorded.

    Counts rather than asserts, so a test can state how many listeners a call created and
    how often each ran — the real subscriber refuses a second start outright.
    """

    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self) -> None:
        self.stop_calls += 1


class FakeHistoryListenerFactory:
    """Stands in for a HistoryListenerFactory, keeping every listener it handed out."""

    def __init__(self) -> None:
        self.listeners: list[FakeHistoryListener] = []

    def create(self, run_info: RunInfo, handler: Callable[[HistoryEvent], None]) -> FakeHistoryListener:
        listener = FakeHistoryListener()
        self.listeners.append(listener)
        return listener


class FakeKVApi:
    async def load(self, key: str, ty: type | None = None) -> Any:
        return None


def make_context(  # noqa: PLR0913
    step_history: list[HistoryEvent] | None = None,
    *,
    appender: FakeAppender | None = None,
    workflow_api: FakeWorkflowAPI | None = None,
    listener_factory: FakeHistoryListenerFactory | None = None,
    run_info: RunInfo = DEFAULT_RUN_INFO,
    worker_id: str = DEFAULT_WORKER_ID,
    parent_run: RunInfo | None = None,
    childs: ChildTracker | None = None,
    codec: Codec | None = None,
    step_infos: dict[str, StepInfo] | None = None,
    store: Store | None = None,
) -> Context:
    """Build a Context wired to fakes, over a fresh journal seeded with `step_history`.

    Pass your own `appender` when you need to assert on what got recorded — Context
    keeps its journal private, so the appender you hand in is the only handle onto that.
    """
    journal = Journal(step_history=step_history or [], appender=appender if appender is not None else FakeAppender())
    directive = Directive(
        id="directive-1",
        timestamp=run_info.created_at,
        kind=DirectiveKind.step,
        run_info=run_info,
        msg=Step(step_name="current_step"),
    )
    drc_factory = DrcFactory(
        run_info,
        worker_id,
        directive,
        step_infos if step_infos is not None else {"current_step": StepInfo(timeout_ms=0)},
    )
    context_codec = codec if codec is not None else MsgspecCodec()
    kvman = store if store is not None else KVManager(FakeKVApi(), context_codec)
    context_workflow_api = workflow_api if workflow_api is not None else FakeWorkflowAPI()
    context_listener_factory = listener_factory if listener_factory is not None else FakeHistoryListenerFactory()
    handle_factory = WorkflowHandleFactory(
        workflow_api=context_workflow_api,
        listener_factory=context_listener_factory,
        sender_id=worker_id,
        logger=logging.getLogger("tests.exec"),
        decoder=context_codec,
    )
    return Context(
        journal,
        run_info,
        worker_id,
        "current_step",
        drc_factory,
        kvman,
        workflow_api=context_workflow_api,
        handle_factory=handle_factory,
        childs=childs if childs is not None else ChildTracker(),
        codec=context_codec,
        logger=build_workflow_logger(journal, run_info, worker_id, "current_step"),
        parent_run=parent_run,
    )


@dataclass
class RecordReplayResult:
    """Outcome of `record_then_replay`, for assertions specific to one operation."""

    value: Any
    events: list[HistoryEvent]
    replay_events: list[HistoryEvent]


async def record_then_replay(
    context: Callable[..., Context],
    call: Callable[[Context], Awaitable[Any]],
) -> RecordReplayResult:
    """Run `call` once to record history, then again replaying that history.

    `context(step_history, appender=...)` is invoked once per run (record, then
    replay); a factory that also stashes state (e.g. the FakeConnection it built)
    on each call lets a test inspect record-time vs. replay-time state separately.

    Asserts the universal replay invariant: same value, and replay commits no new
    events. Callers assert anything operation-specific (event kind, publish counts)
    against the returned events.
    """
    appender = FakeAppender()
    ctx = context(None, appender=appender)
    value = await call(ctx)

    history = appender.events  # becomes the replay run's step history
    replay_appender = FakeAppender()
    replay_ctx = context(history, appender=replay_appender)
    replayed_value = await call(replay_ctx)

    assert replayed_value == value
    assert replay_appender.events == []

    return RecordReplayResult(value=value, events=appender.events, replay_events=replay_appender.events)
