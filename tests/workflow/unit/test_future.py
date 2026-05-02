import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from grctl.models import (
    ErrorDetails,
    HistoryEvent,
    HistoryKind,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunInfo,
    RunScheduled,
    RunStarted,
    RunTimeout,
    StepStarted,
)
from grctl.models.errors import WorkflowError
from grctl.workflow.future import WorkflowFuture, create_workflow_future


@pytest.fixture
def run_info():
    return RunInfo(
        id=str(ULID()),
        wf_id=str(ULID()),
        wf_type="TestWorkflow",
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def mock_nc():
    nc = AsyncMock()
    nc.jetstream = MagicMock(return_value=AsyncMock())
    return nc


@pytest.fixture
def workflow_future(run_info, mock_nc):
    with patch("grctl.workflow.future.HistorySubscriber") as mock_sub:
        mock_subscriber = AsyncMock()
        mock_sub.return_value = mock_subscriber
        future = WorkflowFuture(run_info=run_info, nc=mock_nc, payload={"test": "data"})
        future._subscriber = mock_subscriber
        return future


class TestWorkflowFutureInit:
    def test_init_with_payload(self, run_info, mock_nc):
        with patch("grctl.workflow.future.HistorySubscriber") as mock_sub:
            mock_subscriber = AsyncMock()
            mock_sub.return_value = mock_subscriber

            payload = {"test": "data"}
            future = WorkflowFuture(run_info=run_info, nc=mock_nc, payload=payload)

            assert future.run_info == run_info
            assert future.payload == payload
            assert not future.done()
            mock_sub.assert_called_once_with(
                nc=mock_nc,
                wf_id=run_info.wf_id,
                run_id=run_info.id,
                handler=future._handle_history_event,
            )

    def test_init_without_payload(self, run_info, mock_nc):
        with patch("grctl.workflow.future.HistorySubscriber") as mock_sub:
            mock_subscriber = AsyncMock()
            mock_sub.return_value = mock_subscriber

            future = WorkflowFuture(run_info=run_info, nc=mock_nc)

            assert future.run_info == run_info
            assert future.payload is None
            assert not future.done()

    def test_history_update_handlers_registered(self, workflow_future):
        handlers = workflow_future._history_update_handlers

        assert HistoryKind.run_scheduled in handlers
        assert HistoryKind.run_started in handlers
        assert HistoryKind.run_completed in handlers
        assert HistoryKind.run_failed in handlers
        assert HistoryKind.run_timeout in handlers
        assert HistoryKind.run_cancelled in handlers


class TestWorkflowFutureStartStop:
    @pytest.mark.asyncio
    async def test_start(self, workflow_future):
        await workflow_future.start()
        workflow_future._subscriber.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_when_not_done(self, workflow_future):
        await workflow_future.stop()

        workflow_future._subscriber.stop.assert_awaited_once()
        assert workflow_future.cancelled()

    @pytest.mark.asyncio
    async def test_stop_when_already_done(self, workflow_future):
        workflow_future.set_result("test_result")

        await workflow_future.stop()

        workflow_future._subscriber.stop.assert_awaited_once()
        assert not workflow_future.cancelled()
        assert workflow_future.result() == "test_result"


class TestWorkflowFutureNonTerminalEvents:
    def test_on_run_scheduled(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_scheduled,
            msg=RunScheduled(),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert not workflow_future.done()

    def test_on_run_started(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_started,
            msg=RunStarted(),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert not workflow_future.done()


class TestWorkflowFutureRunCompleted:
    def test_run_completed_sets_result(self, workflow_future, run_info):
        result_data = {"status": "success", "value": 42}
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_completed,
            msg=RunCompleted(result=result_data, duration_ms=1000),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.done()
        assert workflow_future.result() == result_data

    def test_run_completed_with_none_result(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_completed,
            msg=RunCompleted(result=None, duration_ms=500),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.done()
        assert workflow_future.result() is None

    def test_run_completed_when_already_done(self, workflow_future, run_info):
        workflow_future.set_result("first_result")

        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_completed,
            msg=RunCompleted(result="second_result", duration_ms=800),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.result() == "first_result"

    def test_run_completed_with_wrong_payload_type(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_completed,
            msg=RunFailed(error=ErrorDetails(type="Error", message="test", stack_trace="trace"), duration_ms=100),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert not workflow_future.done()


class TestWorkflowFutureRunFailed:
    def test_run_failed_sets_exception(self, workflow_future, run_info):
        error = ErrorDetails(type="ValueError", message="Invalid input", stack_trace="traceback here")
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_failed,
            msg=RunFailed(error=error, duration_ms=500),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.done()
        with pytest.raises(WorkflowError) as exc_info:
            workflow_future.result()
        assert "ValueError: Invalid input" in str(exc_info.value)

    def test_run_failed_with_multiple_errors(self, workflow_future, run_info):
        error = ErrorDetails(type="CustomError", message="Something went wrong", stack_trace="trace")
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_failed,
            msg=RunFailed(error=error, duration_ms=300),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.done()
        with pytest.raises(WorkflowError) as exc_info:
            workflow_future.result()
        assert "CustomError: Something went wrong" in str(exc_info.value)

    def test_run_failed_with_empty_message(self, workflow_future, run_info):
        error = ErrorDetails(type="Error", message="", stack_trace="trace")
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_failed,
            msg=RunFailed(error=error, duration_ms=200),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.done()
        with pytest.raises(WorkflowError) as exc_info:
            workflow_future.result()
        assert "Error:" in str(exc_info.value)

    def test_run_failed_when_already_done(self, workflow_future, run_info):
        workflow_future.set_result("success")

        error = ErrorDetails(type="Error", message="test", stack_trace="trace")
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_failed,
            msg=RunFailed(error=error, duration_ms=100),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.result() == "success"

    def test_run_failed_with_wrong_payload_type(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_failed,
            msg=RunCompleted(result="wrong", duration_ms=100),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert not workflow_future.done()


class TestWorkflowFutureRunTimeout:
    def test_run_timeout_sets_exception(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_timeout,
            msg=RunTimeout(duration_ms=5000),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.done()
        with pytest.raises(TimeoutError) as exc_info:
            workflow_future.result()
        assert "5000" in str(exc_info.value)

    def test_run_timeout_when_already_done(self, workflow_future, run_info):
        workflow_future.set_result("success")

        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_timeout,
            msg=RunTimeout(duration_ms=1000),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.result() == "success"

    def test_run_timeout_with_wrong_payload_type(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_timeout,
            msg=RunCompleted(result="wrong", duration_ms=100),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert not workflow_future.done()


class TestWorkflowFutureRunCancelled:
    def test_run_cancelled_sets_exception(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_cancelled,
            msg=RunCancelled(duration_ms=200, reason="Test cancellation"),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.done()
        with pytest.raises(asyncio.CancelledError):
            workflow_future.result()

    def test_run_cancelled_when_already_done(self, workflow_future, run_info):
        workflow_future.set_result("success")

        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_cancelled,
            msg=RunCancelled(duration_ms=200, reason="Test cancellation"),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert workflow_future.result() == "success"

    def test_run_cancelled_with_wrong_payload_type(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.run_cancelled,
            msg=RunCompleted(result="wrong", duration_ms=100),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert not workflow_future.done()


class TestWorkflowFutureEventHandling:
    def test_handle_unknown_event_kind(self, workflow_future, run_info):
        event = HistoryEvent(
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id="test-worker",
            kind=HistoryKind.step_started,
            msg=StepStarted(step_name="test_step"),
            timestamp=datetime.now(UTC),
        )

        workflow_future._handle_history_event(event)

        assert not workflow_future.done()

    def test_handle_event_with_exception(self, workflow_future, run_info):
        with patch.object(
            workflow_future,
            "_history_update_handlers",
            {HistoryKind.run_completed: MagicMock(side_effect=ValueError("test error"))},
        ):
            event = HistoryEvent(
                wf_id=run_info.wf_id,
                run_id=run_info.id,
                worker_id="test-worker",
                kind=HistoryKind.run_completed,
                msg=RunCompleted(result="test", duration_ms=100),
                timestamp=datetime.now(UTC),
            )

            workflow_future._handle_history_event(event)

            assert workflow_future.done()
            with pytest.raises(ValueError, match="test error"):
                workflow_future.result()

    def test_handle_event_exception_when_already_done(self, workflow_future, run_info):
        workflow_future.set_result("success")

        with patch.object(
            workflow_future,
            "_history_update_handlers",
            {HistoryKind.run_completed: MagicMock(side_effect=ValueError("test error"))},
        ):
            event = HistoryEvent(
                wf_id=run_info.wf_id,
                run_id=run_info.id,
                worker_id="test-worker",
                kind=HistoryKind.run_completed,
                msg=RunCompleted(result="test", duration_ms=100),
                timestamp=datetime.now(UTC),
            )

            workflow_future._handle_history_event(event)

            assert workflow_future.result() == "success"


class TestCreateWorkflowFuture:
    @pytest.mark.asyncio
    async def test_create_workflow_future(self, run_info, mock_nc):
        with patch("grctl.workflow.future.HistorySubscriber"):
            future = await create_workflow_future(run_info=run_info, nc=mock_nc)

            assert isinstance(future, WorkflowFuture)
            assert future.run_info == run_info
            assert future.payload is None

    @pytest.mark.asyncio
    async def test_create_workflow_future_with_payload(self, run_info, mock_nc):
        with patch("grctl.workflow.future.HistorySubscriber"):
            payload = {"test": "data"}
            future = await create_workflow_future(run_info=run_info, nc=mock_nc, payload=payload)

            assert isinstance(future, WorkflowFuture)
            assert future.run_info == run_info
            assert future.payload == payload
