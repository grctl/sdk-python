"""Shared fakes for workflow unit tests."""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from grctl.models import HistoryEvent, HistoryKind, RunInfo
from grctl.models.history import HistoryEvents

DEFAULT_RUN_INFO = RunInfo(id="run-1", wf_id="wf-1", wf_type="test-workflow")
LOGGER = logging.getLogger("tests.workflow")


@dataclass
class WorkflowAPICall:
    """One recorded run-scoped call, for asserting on intent."""

    op: str
    run_info: RunInfo
    sender_id: str
    kwargs: dict[str, Any] = field(default_factory=dict)


class FakeWorkflowAPI:
    """Records each run-scoped call as intent instead of putting a command on the wire."""

    def __init__(self) -> None:
        self.calls: list[WorkflowAPICall] = []

    async def start_run(self, run_info: RunInfo, input: Any, sender_id: str) -> None:  # noqa: A002
        self.calls.append(WorkflowAPICall("start_run", run_info, sender_id, {"input": input}))

    async def send_event(self, run_info: RunInfo, event_name: str, payload: Any, sender_id: str) -> None:
        self.calls.append(
            WorkflowAPICall("send_event", run_info, sender_id, {"event_name": event_name, "payload": payload})
        )

    async def cancel_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None:
        self.calls.append(WorkflowAPICall("cancel_run", run_info, sender_id, {"reason": reason}))

    async def terminate_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None:
        self.calls.append(WorkflowAPICall("terminate_run", run_info, sender_id, {"reason": reason}))


class FakeHistoryListener:
    """No-op stand-in for a HistoryListener, tracking start/stop calls."""

    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self) -> None:
        self.stop_calls += 1


class CapturingListenerFactory:
    """Stands in for a HistoryListenerFactory, capturing the handler so tests can
    deliver synthetic history events directly into the future under test.
    """

    def __init__(self) -> None:
        self.listener = FakeHistoryListener()
        self.handler: Callable[[HistoryEvent], None] | None = None
        self.run_info: RunInfo | None = None

    def create(self, run_info: RunInfo, handler: Callable[[HistoryEvent], None]) -> FakeHistoryListener:
        self.run_info = run_info
        self.handler = handler
        return self.listener

    def deliver(self, event: HistoryEvent) -> None:
        assert self.handler is not None, "listener factory never used to create a listener"
        self.handler(event)


class FakeResultDecoder:
    """Records the raw value and type it was asked to decode, and returns a fixed value."""

    def __init__(self, decoded: object = None) -> None:
        self.decoded = decoded
        self.calls: list[tuple[object, type]] = []

    def from_primitive(self, raw: object, tp: type) -> object:
        self.calls.append((raw, tp))
        return self.decoded


def make_history_event(kind: HistoryKind, msg: HistoryEvents, run_info: RunInfo = DEFAULT_RUN_INFO) -> HistoryEvent:
    return HistoryEvent(
        wf_id=run_info.wf_id,
        run_id=run_info.id,
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=kind,
        msg=msg,
    )
