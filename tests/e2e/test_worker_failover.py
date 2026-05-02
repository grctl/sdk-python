import asyncio
import multiprocessing
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import ulid

from grctl.client.client import Client
from grctl.logging_config import get_logger, setup_logging
from grctl.models import Directive, HistoryEvent, HistoryKind, RunInfo, TaskCompleted
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.task import task
from grctl.worker.worker import Worker
from grctl.workflow import Workflow
from tests.e2e.helpers import _terminate_process, _wait_for_history_event, _wait_until_released

setup_logging()
logger = get_logger(__name__)

_WORKFLOW_TYPE = "FailoverTestWorkflow"
_WORKER_INIT_DELAY = 0.5


async def _run_worker(wf: Workflow, timeout_seconds: float) -> None:
    connection = await Connection.connect()
    worker = Worker(workflows=[wf], connection=connection)
    try:
        await asyncio.wait_for(worker.start(), timeout=timeout_seconds)
    except TimeoutError:
        pass
    except Exception:
        logger.exception("Worker error")


def _failover_worker_process_main(
    execution_counter: Any,
    timeout_seconds: float = 60.0,
    pause_event: Any | None = None,
) -> None:
    wf = Workflow(workflow_type=_WORKFLOW_TYPE)

    @task
    async def counted_task(n: int) -> int:
        with execution_counter.get_lock():
            execution_counter.value += 1
        return n * 10

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(main_step)

    @wf.step()
    async def main_step(ctx: Context) -> Directive:
        r1 = await counted_task(1)
        r2 = await counted_task(2)
        await _wait_until_released(pause_event)
        r3 = await counted_task(3)
        return ctx.next.complete([r1, r2, r3])

    asyncio.run(_run_worker(wf, timeout_seconds))


@pytest.mark.asyncio
async def test_wait_for_history_event_returns_when_event_present() -> None:
    """_wait_for_history_event returns when a matching event is in the stream."""
    connection = await Connection.connect()
    wf_id = str(ulid.ULID())
    run_id = str(ulid.ULID())
    run_info = RunInfo(id=run_id, wf_id=wf_id, wf_type=_WORKFLOW_TYPE)

    event = HistoryEvent(
        wf_id=wf_id,
        run_id=run_id,
        worker_id="test-worker",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.task_completed,
        msg=TaskCompleted(
            task_id="counted_task:abc123",
            task_name="counted_task",
            output={"result": 10},
            step_name="main_step",
            duration_ms=5,
        ),
        operation_id=str(ulid.ULID()),
    )
    await connection.publisher.publish_history(run_info=run_info, event=event)

    try:
        returned = await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=wf_id,
            run_id=run_id,
            kind=HistoryKind.task_completed,
            timeout_s=5.0,
            predicate=lambda e: isinstance(e.msg, TaskCompleted) and e.msg.task_name == "counted_task",
        )
        assert returned.kind == HistoryKind.task_completed
    finally:
        Connection.reset()


@pytest.mark.asyncio
async def test_wait_for_history_event_raises_timeout_when_event_absent() -> None:
    """_wait_for_history_event raises TimeoutError if the event never appears."""
    connection = await Connection.connect()

    try:
        with pytest.raises(TimeoutError):
            await _wait_for_history_event(
                js=connection.js,
                manifest=connection.manifest,
                wf_id=str(ulid.ULID()),
                run_id=str(ulid.ULID()),
                kind=HistoryKind.task_completed,
                timeout_s=0.6,
                predicate=lambda e: isinstance(e.msg, TaskCompleted) and e.msg.task_name == "counted_task",
            )
    finally:
        Connection.reset()


@pytest.mark.asyncio
async def test_failover_baseline() -> None:
    """Single worker, no failover: workflow produces [10, 20, 30] and counter==3."""
    execution_counter = multiprocessing.Value("i", 0)
    connection = await Connection.connect()
    client = Client(connection=connection)

    worker_process = multiprocessing.Process(
        target=_failover_worker_process_main,
        args=(execution_counter,),
        daemon=True,
    )
    worker_process.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    try:
        result = await client.run_workflow(
            type=_WORKFLOW_TYPE,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )
        assert result == [10, 20, 30]
        assert execution_counter.value == 3
    finally:
        _terminate_process(worker_process)
        Connection.reset()


@pytest.mark.asyncio
async def test_worker_failover_replays_completed_tasks() -> None:
    """Worker A killed after task_2 completes; Worker B finishes with [10, 20, 30] and counter==3.

    The pause_event holds Worker A in main_step between task_2 and task_3, so the directive is
    unacked when Worker A is killed. Worker B then receives the re-delivered directive, replays
    task_1 and task_2 from history (no counter increment), and executes task_3 live (counter → 3).

    If replay is broken (task bodies re-execute during replay), the counter reaches 5.
    The ack_wait on the directive consumer must be short (3-5 s) for timely re-delivery.
    """
    execution_counter = multiprocessing.Value("i", 0)
    pause_event = multiprocessing.Event()
    connection = await Connection.connect()
    client = Client(connection=connection)

    worker_a = multiprocessing.Process(
        target=_failover_worker_process_main,
        args=(execution_counter, 60.0, pause_event),
        daemon=True,
    )
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    workflow_id = str(ulid.ULID())
    handle = await client.start_workflow(
        type=_WORKFLOW_TYPE,
        id=workflow_id,
        input={},
        timeout=timedelta(seconds=120),
    )
    run_id = handle.run_info.id

    worker_b = multiprocessing.Process(
        target=_failover_worker_process_main,
        args=(execution_counter,),
        daemon=True,
    )

    try:
        # Wait until task_2 is durable in history before killing Worker A.
        # Worker A is paused between task_2 and task_3, so the directive is guaranteed unacked.
        await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=run_id,
            kind=HistoryKind.task_completed,
            timeout_s=30.0,
            predicate=lambda e: isinstance(e.msg, TaskCompleted) and e.msg.task_name == "counted_task",
            occurrence=1,
        )

        _terminate_process(worker_a)

        # Worker B picks up the re-delivered directive after ack_wait expires.
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=60.0)
        await handle.future.stop()

        assert result == [10, 20, 30], f"Expected [10, 20, 30], got {result}"
        assert execution_counter.value == 3, (
            f"Expected counter=3 (each task body runs exactly once), got {execution_counter.value}. "
            "counter==5 means replay is broken and task bodies re-executed."
        )
    finally:
        _terminate_process(worker_a)
        _terminate_process(worker_b)
        Connection.reset()
