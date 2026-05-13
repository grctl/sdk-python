import contextvars
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from grctl.models import Directive, HistoryEvent, HistoryKind, TaskCompleted
from grctl.nats.connection import Connection
from grctl.worker.logger import ReplayAwareLogger, _is_replaying
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


class TestIsReplayingHelper:
    def test_false_when_no_runtime_set(self):
        # Run in a fresh context where the ContextVar has no value
        result = contextvars.copy_context().run(_is_replaying)
        assert result is False

    def test_true_when_replaying(self):
        runtime = _make_runtime(step_history=[_make_event("op-1")])
        set_step_runtime(runtime)
        assert _is_replaying() is True

    def test_false_when_not_replaying(self):
        runtime = _make_runtime(step_history=[])
        set_step_runtime(runtime)
        assert _is_replaying() is False


class TestReplayAwareLogger:
    def setup_method(self):
        self.logger = ReplayAwareLogger("order_wf")

    def test_logger_name(self):
        assert self.logger._logger.name == "grctl.workflow.order_wf"

    @pytest.mark.parametrize("method", ["debug", "info", "warning", "error", "critical", "exception"])
    def test_suppressed_during_replay(self, method: str):
        runtime = _make_runtime(step_history=[_make_event("op-1")])
        set_step_runtime(runtime)
        with patch.object(self.logger._logger, method) as mock_method:
            getattr(self.logger, method)("test message")
            mock_method.assert_not_called()

    @pytest.mark.parametrize("method", ["debug", "info", "warning", "error", "critical"])
    def test_forwarded_when_not_replaying(self, method: str):
        runtime = _make_runtime(step_history=[])
        set_step_runtime(runtime)
        with patch.object(self.logger._logger, method) as mock_method:
            getattr(self.logger, method)("test message")
            mock_method.assert_called_once_with("test message")

    def test_exception_forwarded_when_not_replaying(self):
        runtime = _make_runtime(step_history=[])
        set_step_runtime(runtime)
        with patch.object(self.logger._logger, "exception") as mock_method:
            self.logger.exception("test error")
            mock_method.assert_called_once_with("test error")
