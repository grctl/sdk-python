from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from grctl.models import Directive, DirectiveKind, RunInfo, Start, TaskCompleted
from grctl.models.history import HistoryEvent, HistoryKind
from grctl.worker.run_manager import RunManager
from grctl.workflow import Workflow


def _directive(*, attempt: int = 0, history_seq_id: int = 0) -> Directive:
    return Directive(
        id="dir-1",
        timestamp=datetime.now(UTC),
        kind=DirectiveKind.start,
        run_info=RunInfo(
            id="run-1",
            wf_id="wf-1",
            wf_type="TestWorkflow",
            created_at=datetime.now(UTC),
            history_seq_id=history_seq_id,
        ),
        msg=Start(input=None),
        attempt=attempt,
    )


def _history_event() -> HistoryEvent:
    return HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.task_completed,
        msg=TaskCompleted(
            task_id="task-1",
            task_name="fetch_data",
            output={"ok": True},
            step_name="start",
            duration_ms=3,
        ),
        operation_id="fetch_data:1",
    )


@pytest.mark.asyncio
async def test_handle_next_directive_fetches_step_history_for_replay(mock_connection) -> None:
    connection, _ = mock_connection
    workflow = Workflow(workflow_type="TestWorkflow")
    manager = RunManager("worker-name", "worker-1", [workflow], connection)
    directive = _directive(attempt=1, history_seq_id=42)
    history = [_history_event()]

    with (
        patch("grctl.worker.run_manager.fetch_step_history", new=AsyncMock(return_value=history)) as fetch_history,
        patch("grctl.worker.run_manager.WorkflowRunner") as runner_cls,
    ):
        runner = Mock()
        runner.runtime = None
        runner_cls.side_effect = lambda runtime: Mock(runtime=runtime)
        manager._start_task = Mock(return_value=None)  # ty:ignore[invalid-assignment]

        await manager.handle_next_directive(directive)

    fetch_history.assert_awaited_once_with(
        js=connection.js,
        manifest=connection.manifest,
        wf_id="wf-1",
        run_id="run-1",
        history_seq_id=42,
    )
    runtime = manager._start_task.call_args.args[0].runtime  # ty:ignore[unresolved-attribute]
    assert runtime.step_history == history


@pytest.mark.asyncio
async def test_handle_next_directive_skips_fetch_on_first_attempt(mock_connection) -> None:
    connection, _ = mock_connection
    workflow = Workflow(workflow_type="TestWorkflow")
    manager = RunManager("worker-name", "worker-1", [workflow], connection)
    directive = _directive(attempt=0, history_seq_id=42)

    with (
        patch("grctl.worker.run_manager.fetch_step_history", new=AsyncMock()) as fetch_history,
        patch("grctl.worker.run_manager.WorkflowRunner", side_effect=lambda runtime: Mock(runtime=runtime)),
    ):
        manager._start_task = Mock(return_value=None)  # ty:ignore[invalid-assignment]
        await manager.handle_next_directive(directive)

    fetch_history.assert_not_awaited()
    runtime = manager._start_task.call_args.args[0].runtime  # ty:ignore[unresolved-attribute]
    assert runtime.step_history == []


@pytest.mark.asyncio
async def test_handle_next_directive_skips_fetch_when_history_seq_id_missing(mock_connection) -> None:
    connection, _ = mock_connection
    workflow = Workflow(workflow_type="TestWorkflow")
    manager = RunManager("worker-name", "worker-1", [workflow], connection)
    directive = _directive(attempt=2, history_seq_id=0)

    with (
        patch("grctl.worker.run_manager.fetch_step_history", new=AsyncMock()) as fetch_history,
        patch("grctl.worker.run_manager.WorkflowRunner", side_effect=lambda runtime: Mock(runtime=runtime)),
    ):
        manager._start_task = Mock(return_value=None)  # ty:ignore[invalid-assignment]
        await manager.handle_next_directive(directive)

    fetch_history.assert_not_awaited()
    runtime = manager._start_task.call_args.args[0].runtime  # ty:ignore[unresolved-attribute]
    assert runtime.step_history == []
