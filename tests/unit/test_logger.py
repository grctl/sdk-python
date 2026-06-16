import contextvars
import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

from grctl.models import Directive, HistoryEvent, HistoryKind, TaskCompleted
from grctl.nats.connection import Connection
from grctl.worker.logger import ReplayFilter
from grctl.worker.runtime import StepRuntime, set_step_runtime
from grctl.workflow import Workflow


def _make_runtime(step_history: list[HistoryEvent] | None = None) -> StepRuntime:
    return StepRuntime(
        workflow=Mock(spec=Workflow),
        worker_id="test-worker",
        directive=Mock(spec=Directive),
        connection=AsyncMock(spec=Connection),
        step_history=step_history if step_history is not None else [],
    )


def _make_event(operation_id: str) -> HistoryEvent:
    return HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="w-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.task_completed,
        msg=TaskCompleted(task_id="t-1", task_name="t", output={"result": None}, step_name="s", duration_ms=1),
        operation_id=operation_id,
    )


class TestIsReplayingProperty:
    def test_false_when_no_history(self):
        runtime = _make_runtime(step_history=[])
        assert runtime.is_replaying is False

    def test_false_when_history_is_none(self):
        runtime = _make_runtime(step_history=None)
        assert runtime.is_replaying is False

    def test_true_when_cursor_within_history(self):
        runtime = _make_runtime(step_history=[_make_event("op-1")])
        assert runtime.is_replaying is True

    def test_false_when_cursor_past_history(self):
        runtime = _make_runtime(step_history=[_make_event("op-1")])
        runtime._cursor = 1
        assert runtime.is_replaying is False


class TestReplayFilter:
    def _make_record(self) -> logging.LogRecord:
        return logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )

    def test_allows_when_not_replaying(self):
        f = ReplayFilter(is_replaying=lambda: False)
        assert f.filter(self._make_record()) is True

    def test_suppresses_when_replaying(self):
        f = ReplayFilter(is_replaying=lambda: True)
        assert f.filter(self._make_record()) is False

    def test_filter_reflects_runtime_state(self):
        runtime = _make_runtime(step_history=[_make_event("op-1")])
        set_step_runtime(runtime)
        f = ReplayFilter(is_replaying=lambda: runtime.is_replaying)

        assert f.filter(self._make_record()) is False  # replaying

        runtime._cursor = 1
        assert f.filter(self._make_record()) is True  # past history

    def test_filter_false_when_no_runtime(self):
        def safe_check() -> bool:
            try:
                from grctl.worker.runtime import get_step_runtime  # noqa: PLC0415

                return get_step_runtime().is_replaying
            except LookupError:
                return False

        f = ReplayFilter(is_replaying=safe_check)
        result = contextvars.copy_context().run(f.filter, self._make_record())
        assert result is True  # no runtime → not replaying → allow
