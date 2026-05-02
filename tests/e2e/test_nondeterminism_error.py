import asyncio
import multiprocessing
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import ulid

from grctl.client.client import Client
from grctl.logging_config import get_logger, setup_logging
from grctl.models import ChildWorkflowStarted, Directive, HistoryKind, RunInfo, TaskCompleted
from grctl.models.errors import WorkflowError
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.task import task
from grctl.worker.worker import Worker
from grctl.workflow import Workflow, WorkflowHandle
from tests.e2e.helpers import _terminate_process, _wait_for_history_event, _wait_until_released

setup_logging()
logger = get_logger(__name__)

_WORKER_INIT_DELAY = 0.5
_POLL_TIMEOUT = 30.0
_WORKFLOW_TIMEOUT = timedelta(seconds=120)

_NDET_TASK_INPUT_WORKFLOW_TYPE = "NonDeterminismTaskInputWorkflow"
_NDET_TASK_REORDER_WORKFLOW_TYPE = "NonDeterminismTaskReorderWorkflow"
_NDET_START_PARENT_WORKFLOW_TYPE = "NonDeterminismStartParentWorkflow"
_NDET_START_CHILD_WORKFLOW_TYPE = "NonDeterminismStartChildWorkflow"
_NDET_SEND_PARENT_WORKFLOW_TYPE = "NonDeterminismSendParentWorkflow"
_NDET_SEND_CHILD_WORKFLOW_TYPE = "NonDeterminismSendChildWorkflow"


async def _run_worker(workflows: list[Workflow], timeout_seconds: float) -> None:
    connection = await Connection.connect()
    worker = Worker(workflows=workflows, connection=connection)
    try:
        await asyncio.wait_for(worker.start(), timeout=timeout_seconds)
    except TimeoutError:
        pass
    except Exception:
        logger.exception("Worker error")


async def _assert_workflow_fails_with_nondeterminism(handle: WorkflowHandle, future_timeout: float = 60.0) -> None:
    with pytest.raises(WorkflowError) as exc_info:
        await asyncio.wait_for(handle.future, timeout=future_timeout)
    assert "NonDeterminismError" in str(exc_info.value), str(exc_info.value)


def _build_task_input_workflow(task_value: str, pause_event: Any | None) -> Workflow:
    wf = Workflow(workflow_type=_NDET_TASK_INPUT_WORKFLOW_TYPE)

    @task
    async def single_task(value: str) -> str:
        return value.upper()

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(main_step)

    @wf.step()
    async def main_step(ctx: Context) -> Directive:
        result = await single_task(value=task_value)
        await _wait_until_released(pause_event)
        return ctx.next.complete(result)

    return wf


def _ndet_task_input_v1_worker_main(pause_event: Any, timeout_seconds: float = 60.0) -> None:
    asyncio.run(_run_worker([_build_task_input_workflow("hello", pause_event)], timeout_seconds))


def _ndet_task_input_v2_worker_main(timeout_seconds: float = 60.0) -> None:
    asyncio.run(_run_worker([_build_task_input_workflow("world", None)], timeout_seconds))


def _build_task_reorder_workflow(pause_event: Any | None, reordered: bool) -> Workflow:
    wf = Workflow(workflow_type=_NDET_TASK_REORDER_WORKFLOW_TYPE)

    @task
    async def task_one() -> str:
        return "first"

    @task
    async def task_two() -> str:
        return "second"

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(main_step)

    @wf.step()
    async def main_step(ctx: Context) -> Directive:
        if reordered:
            first = await task_two()
            second = await task_one()
        else:
            first = await task_one()
            await _wait_until_released(pause_event)
            second = await task_two()
        return ctx.next.complete([first, second])

    return wf


def _ndet_task_reorder_v1_worker_main(pause_event: Any, timeout_seconds: float = 60.0) -> None:
    asyncio.run(_run_worker([_build_task_reorder_workflow(pause_event, reordered=False)], timeout_seconds))


def _ndet_task_reorder_v2_worker_main(timeout_seconds: float = 60.0) -> None:
    asyncio.run(_run_worker([_build_task_reorder_workflow(None, reordered=True)], timeout_seconds))


def _build_start_parent_workflow(child_id_suffix: str, pause_event: Any | None) -> Workflow:
    parent = Workflow(workflow_type=_NDET_START_PARENT_WORKFLOW_TYPE)

    @parent.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(main_step)

    @parent.step()
    async def main_step(ctx: Context) -> Directive:
        child_workflow_id = f"{ctx.run.wf_id}-{child_id_suffix}"
        await ctx.start(_NDET_START_CHILD_WORKFLOW_TYPE, child_workflow_id)
        await _wait_until_released(pause_event)
        return ctx.next.complete("started")

    return parent


def _build_start_child_workflow() -> Workflow:
    child = Workflow(workflow_type=_NDET_START_CHILD_WORKFLOW_TYPE)

    @child.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete("child-complete")

    return child


def _ndet_start_v1_worker_main(pause_event: Any, timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_start_parent_workflow("child-v1", pause_event),
                _build_start_child_workflow(),
            ],
            timeout_seconds,
        )
    )


def _ndet_start_v2_worker_main(timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_start_parent_workflow("child-v2", None),
                _build_start_child_workflow(),
            ],
            timeout_seconds,
        )
    )


def _build_send_parent_workflow() -> Workflow:
    parent = Workflow(workflow_type=_NDET_SEND_PARENT_WORKFLOW_TYPE)

    @parent.start()
    async def start(ctx: Context) -> Directive:
        child_workflow_id = f"{ctx.run.wf_id}-child"
        await ctx.start(_NDET_SEND_CHILD_WORKFLOW_TYPE, child_workflow_id)
        return ctx.next.wait_for_event(timeout=timedelta(seconds=5), timeout_step_name="parent_timeout")

    @parent.event(name="status_v1")
    async def on_status_v1(ctx: Context) -> Directive:
        return ctx.next.complete("status-v1")

    @parent.event(name="status_v2")
    async def on_status_v2(ctx: Context) -> Directive:
        return ctx.next.complete("status-v2")

    @parent.step()
    async def parent_timeout(ctx: Context) -> Directive:
        return ctx.next.complete("timeout")

    return parent


def _build_send_child_workflow(event_name: str, pause_event: Any | None) -> Workflow:
    child = Workflow(workflow_type=_NDET_SEND_CHILD_WORKFLOW_TYPE)

    @child.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(send_status)

    @child.step()
    async def send_status(ctx: Context) -> Directive:
        await ctx.send_to_parent(event_name)
        await _wait_until_released(pause_event)
        return ctx.next.complete("sent")

    return child


def _ndet_send_to_parent_v1_worker_main(pause_event: Any, timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_send_parent_workflow(),
                _build_send_child_workflow("status_v1", pause_event),
            ],
            timeout_seconds,
        )
    )


def _ndet_send_to_parent_v2_worker_main(timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_send_parent_workflow(),
                _build_send_child_workflow("status_v2", None),
            ],
            timeout_seconds,
        )
    )


@pytest.mark.asyncio
async def test_task_input_change_raises_nondeterminism() -> None:
    pause_event = multiprocessing.Event()
    connection = await Connection.connect()
    client = Client(connection=connection)
    worker_a = multiprocessing.Process(target=_ndet_task_input_v1_worker_main, args=(pause_event,), daemon=True)
    worker_b = multiprocessing.Process(target=_ndet_task_input_v2_worker_main, daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    workflow_id = str(ulid.ULID())
    handle = await client.start_workflow(
        type=_NDET_TASK_INPUT_WORKFLOW_TYPE,
        id=workflow_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    try:
        await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.task_completed,
            timeout_s=_POLL_TIMEOUT,
            predicate=lambda event: isinstance(event.msg, TaskCompleted) and event.msg.task_name == "single_task",
        )

        _terminate_process(worker_a)
        worker_b.start()

        await _assert_workflow_fails_with_nondeterminism(handle)
    finally:
        await handle.future.stop()
        _terminate_process(worker_a)
        _terminate_process(worker_b)
        Connection.reset()


@pytest.mark.asyncio
async def test_task_reorder_raises_nondeterminism() -> None:
    pause_event = multiprocessing.Event()
    connection = await Connection.connect()
    client = Client(connection=connection)
    worker_a = multiprocessing.Process(target=_ndet_task_reorder_v1_worker_main, args=(pause_event,), daemon=True)
    worker_b = multiprocessing.Process(target=_ndet_task_reorder_v2_worker_main, daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    workflow_id = str(ulid.ULID())
    handle = await client.start_workflow(
        type=_NDET_TASK_REORDER_WORKFLOW_TYPE,
        id=workflow_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    try:
        await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.task_completed,
            timeout_s=_POLL_TIMEOUT,
            predicate=lambda event: isinstance(event.msg, TaskCompleted) and event.msg.task_name == "task_one",
        )

        _terminate_process(worker_a)
        worker_b.start()

        await _assert_workflow_fails_with_nondeterminism(handle)
    finally:
        await handle.future.stop()
        _terminate_process(worker_a)
        _terminate_process(worker_b)
        Connection.reset()


@pytest.mark.asyncio
async def test_start_input_change_raises_nondeterminism() -> None:
    pause_event = multiprocessing.Event()
    connection = await Connection.connect()
    client = Client(connection=connection)
    worker_a = multiprocessing.Process(target=_ndet_start_v1_worker_main, args=(pause_event,), daemon=True)
    worker_b = multiprocessing.Process(target=_ndet_start_v2_worker_main, daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    workflow_id = str(ulid.ULID())
    handle = await client.start_workflow(
        type=_NDET_START_PARENT_WORKFLOW_TYPE,
        id=workflow_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    try:
        await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.child_started,
            timeout_s=_POLL_TIMEOUT,
            predicate=lambda event: isinstance(event.msg, ChildWorkflowStarted),
        )

        _terminate_process(worker_a)
        worker_b.start()

        await _assert_workflow_fails_with_nondeterminism(handle)
    finally:
        await handle.future.stop()
        _terminate_process(worker_a)
        _terminate_process(worker_b)
        Connection.reset()


@pytest.mark.asyncio
async def test_send_to_parent_input_change_raises_nondeterminism() -> None:
    pause_event = multiprocessing.Event()
    connection = await Connection.connect()
    client = Client(connection=connection)
    worker_a = multiprocessing.Process(target=_ndet_send_to_parent_v1_worker_main, args=(pause_event,), daemon=True)
    worker_b = multiprocessing.Process(target=_ndet_send_to_parent_v2_worker_main, daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    workflow_id = str(ulid.ULID())
    parent_handle = await client.start_workflow(
        type=_NDET_SEND_PARENT_WORKFLOW_TYPE,
        id=workflow_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )
    child_handle: WorkflowHandle | None = None

    try:
        child_started_event = await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=parent_handle.run_info.id,
            kind=HistoryKind.child_started,
            timeout_s=_POLL_TIMEOUT,
            predicate=lambda event: isinstance(event.msg, ChildWorkflowStarted),
        )
        child_started = child_started_event.msg
        assert isinstance(child_started, ChildWorkflowStarted)

        child_handle = WorkflowHandle(
            run_info=RunInfo(
                id=child_started.run_id,
                wf_id=child_started.wf_id,
                wf_type=child_started.wf_type,
                created_at=datetime.now(UTC),
            ),
            payload=None,
            connection=connection,
        )
        await child_handle.future.start()

        await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=child_started.wf_id,
            run_id=child_started.run_id,
            kind=HistoryKind.parent_event_sent,
            timeout_s=_POLL_TIMEOUT,
        )

        _terminate_process(worker_a)
        worker_b.start()

        await _assert_workflow_fails_with_nondeterminism(child_handle)
    finally:
        if child_handle is not None:
            await child_handle.future.stop()
        await parent_handle.future.stop()
        _terminate_process(worker_a)
        _terminate_process(worker_b)
        Connection.reset()
