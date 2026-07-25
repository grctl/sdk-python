"""Shared fakes for workflow unit tests."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime

from grctl.models import Command, HistoryEvent, HistoryKind, RunInfo
from grctl.models.history import HistoryEvents

DEFAULT_RUN_INFO = RunInfo(id="run-1", wf_id="wf-1", wf_type="test-workflow")
LOGGER = logging.getLogger("tests.workflow")


class FakeCommandSender:
    """Records every command handed to it instead of putting it on the wire."""

    def __init__(self) -> None:
        self.sent: list[Command] = []

    async def send(self, run: RunInfo, cmd: Command) -> bytes:
        self.sent.append(cmd)
        return b""


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
