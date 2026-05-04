import asyncio
import multiprocessing
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import ulid

from grctl.client.client import Client
from grctl.logging_config import get_logger, setup_logging
from grctl.models import ChildWorkflowStarted, Directive, HistoryKind, RunInfo
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.worker import Worker
from grctl.workflow import Workflow, WorkflowHandle
from tests.e2e.helpers import _terminate_process, _wait_for_history_event, _wait_until_released

setup_logging()
logger = get_logger(__name__)

_WORKER_INIT_DELAY = 0.5
_POLL_TIMEOUT = 30.0
_WORKFLOW_TIMEOUT = timedelta(seconds=120)

_REPLAY_START_PARENT_WORKFLOW_TYPE = "ChildWorkflowReplayStartParentWorkflow"
_REPLAY_START_CHILD_WORKFLOW_TYPE = "ChildWorkflowReplayStartChildWorkflow"
_REPLAY_SEND_PARENT_WORKFLOW_TYPE = "ChildWorkflowReplaySendParentWorkflow"
_REPLAY_SEND_CHILD_WORKFLOW_TYPE = "ChildWorkflowReplaySendChildWorkflow"


async def _run_worker(workflows: list[Workflow], timeout_seconds: float) -> None:
    connection = await Connection.connect()
    worker = Worker(workflows=workflows, connection=connection)
    try:
        await asyncio.wait_for(worker.start(), timeout=timeout_seconds)
    except TimeoutError:
        pass
    except Exception:
        logger.exception("Worker error")


def _build_start_parent_workflow(pause_event: Any | None) -> Workflow:
    parent = Workflow(workflow_type=_REPLAY_START_PARENT_WORKFLOW_TYPE)

    @parent.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(main_step)

    @parent.step()
    async def main_step(ctx: Context) -> Directive:
        child_workflow_id = f"{ctx.run.wf_id}-child"
        await ctx.start(_REPLAY_START_CHILD_WORKFLOW_TYPE, child_workflow_id)
        await _wait_until_released(pause_event)
        return ctx.next.complete("done")

    return parent


def _build_start_child_workflow() -> Workflow:
    child = Workflow(workflow_type=_REPLAY_START_CHILD_WORKFLOW_TYPE)

    @child.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete("child-done")

    return child


def _start_replay_worker_a_main(pause_event: Any, timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_start_parent_workflow(pause_event),
                _build_start_child_workflow(),
            ],
            timeout_seconds,
        )
    )


def _start_replay_worker_b_main(timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_start_parent_workflow(None),
                _build_start_child_workflow(),
            ],
            timeout_seconds,
        )
    )


def _build_send_parent_workflow() -> Workflow:
    parent = Workflow(workflow_type=_REPLAY_SEND_PARENT_WORKFLOW_TYPE)

    @parent.start()
    async def start(ctx: Context) -> Directive:
        child_workflow_id = f"{ctx.run.wf_id}-child"
        await ctx.start(_REPLAY_SEND_CHILD_WORKFLOW_TYPE, child_workflow_id)
        return ctx.next.wait_for_event(timeout=timedelta(seconds=5), timeout_step_name="parent_timeout")

    @parent.event(name="child_done")
    async def on_child_done(ctx: Context) -> Directive:
        return ctx.next.complete("parent-done")

    @parent.step()
    async def parent_timeout(ctx: Context) -> Directive:
        return ctx.next.complete("timeout")

    return parent


def _build_send_child_workflow(pause_event: Any | None) -> Workflow:
    child = Workflow(workflow_type=_REPLAY_SEND_CHILD_WORKFLOW_TYPE)

    @child.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(send_step)

    @child.step()
    async def send_step(ctx: Context) -> Directive:
        await ctx.send_to_parent("child_done")
        await _wait_until_released(pause_event)
        return ctx.next.complete("child-done")

    return child


def _send_replay_worker_a_main(pause_event: Any, timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_send_parent_workflow(),
                _build_send_child_workflow(pause_event),
            ],
            timeout_seconds,
        )
    )


def _send_replay_worker_b_main(timeout_seconds: float = 60.0) -> None:
    asyncio.run(
        _run_worker(
            [
                _build_send_parent_workflow(),
                _build_send_child_workflow(None),
            ],
            timeout_seconds,
        )
    )


def _child_handle_from_started(connection: Connection, child_started: ChildWorkflowStarted) -> WorkflowHandle:
    return WorkflowHandle(
        run_info=RunInfo(
            id=child_started.run_id,
            wf_id=child_started.wf_id,
            wf_type=child_started.wf_type,
            created_at=datetime.now(UTC),
        ),
        payload=None,
        connection=connection,
    )


@pytest.mark.asyncio
async def test_start_replay_skips_duplicate_child_launch() -> None:
    pause_event = multiprocessing.Event()
    connection = await Connection.connect()
    client = Client(connection=connection)
    worker_a = multiprocessing.Process(target=_start_replay_worker_a_main, args=(pause_event,), daemon=True)
    worker_b = multiprocessing.Process(target=_start_replay_worker_b_main, daemon=True)
    child_handle: WorkflowHandle | None = None
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    workflow_id = str(ulid.ULID())
    parent_handle = await client.start_workflow(
        type=_REPLAY_START_PARENT_WORKFLOW_TYPE,
        id=workflow_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

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

        child_handle = _child_handle_from_started(connection, child_started)
        await child_handle.future.start()

        _terminate_process(worker_a)
        worker_b.start()

        parent_result = await asyncio.wait_for(parent_handle.future, timeout=60.0)
        child_result = await asyncio.wait_for(child_handle.future, timeout=60.0)

        assert parent_result == "done"
        assert child_result == "child-done"

        with pytest.raises(TimeoutError):
            await _wait_for_history_event(
                js=connection.js,
                manifest=connection.manifest,
                wf_id=workflow_id,
                run_id=parent_handle.run_info.id,
                kind=HistoryKind.child_started,
                timeout_s=2.0,
                predicate=lambda event: isinstance(event.msg, ChildWorkflowStarted),
                occurrence=1,
            )
    finally:
        if child_handle is not None:
            await child_handle.future.stop()
        await parent_handle.future.stop()
        _terminate_process(worker_a)
        _terminate_process(worker_b)
        Connection.reset()


@pytest.mark.asyncio
async def test_send_to_parent_replay_skips_duplicate_parent_event() -> None:
    pause_event = multiprocessing.Event()
    connection = await Connection.connect()
    client = Client(connection=connection)
    worker_a = multiprocessing.Process(target=_send_replay_worker_a_main, args=(pause_event,), daemon=True)
    worker_b = multiprocessing.Process(target=_send_replay_worker_b_main, daemon=True)
    child_handle: WorkflowHandle | None = None
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    workflow_id = str(ulid.ULID())
    parent_handle = await client.start_workflow(
        type=_REPLAY_SEND_PARENT_WORKFLOW_TYPE,
        id=workflow_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

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

        child_handle = _child_handle_from_started(connection, child_started)
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

        child_result = await asyncio.wait_for(child_handle.future, timeout=60.0)
        parent_result = await asyncio.wait_for(parent_handle.future, timeout=60.0)

        assert child_result == "child-done"
        assert parent_result == "parent-done"

        with pytest.raises(TimeoutError):
            await _wait_for_history_event(
                js=connection.js,
                manifest=connection.manifest,
                wf_id=child_started.wf_id,
                run_id=child_started.run_id,
                kind=HistoryKind.parent_event_sent,
                timeout_s=2.0,
                occurrence=1,
            )
    finally:
        if child_handle is not None:
            await child_handle.future.stop()
        await parent_handle.future.stop()
        _terminate_process(worker_a)
        _terminate_process(worker_b)
        Connection.reset()
