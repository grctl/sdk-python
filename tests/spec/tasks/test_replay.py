import asyncio
import multiprocessing
import os
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.directive import RetryPolicy
from grctl.models.errors import WorkflowError
from grctl.nats.connection import Connection
from grctl.worker import Context, task
from grctl.worker.worker import Worker
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess

_WORKER_INIT_DELAY = 0.5
_HISTORY_TIMEOUT = 15.0
_WORKFLOW_TIMEOUT = timedelta(seconds=120)

# Reduce the time server re reliver the task to another worker after a worker failure
# Speeds up 2 worker tests
_REPLAY_WORKER_ACK_WAIT_SECONDS = "0.5"

_TASK_HISTORY_KINDS = {
    HistoryKind.task_started,
    HistoryKind.task_completed,
    HistoryKind.task_attempt_failed,
    HistoryKind.task_failed,
    HistoryKind.task_cancelled,
}


def _terminate(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=5.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=1.0)


def _configure_fast_replay_redelivery() -> None:
    os.environ.setdefault("ENGINE_NATS_WORKER_ACK_WAIT", _REPLAY_WORKER_ACK_WAIT_SECONDS)


def _unique_wf_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


# ─── Worker process functions ──────────────────────────────────────────────────
# Can be pickled for multiprocessing.
# Each worker creates its own NATS connection and Worker instance.


def _replay_worker(wf_type: str, pause_event=None) -> None:
    """Worker with one task. Pauses before completing the step when pause_event is set.

    Worker A holds the directive unacked while paused — killing it simulates a crash.
    Worker B picks up the re-delivered directive and replays completed tasks from history.
    """

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task
        async def simple_task() -> str:
            return "done"

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            result = await simple_task()
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete(result)

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _replay_exception_worker(wf_type: str, pause_event=None) -> None:
    """Worker with a task that always fails. Pauses in the exception handler."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task(retry_policy=RetryPolicy(max_attempts=2, initial_delay_ms=1, backoff_multiplier=1.0))
        async def failing_task() -> str:
            raise ValueError("original error")

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            try:
                await failing_task()
            except ValueError:
                if pause_event is not None:
                    await asyncio.to_thread(pause_event.wait)
                raise
            return ctx.next.complete("unreachable")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _replay_cancelled_worker(wf_type: str, pause_event=None) -> None:
    """Worker with a task that cancels. Pauses after handling the cancellation."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task
        async def cancelled_task() -> str:
            raise asyncio.CancelledError("cancel now")

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            try:
                await cancelled_task()
            except asyncio.CancelledError:
                if pause_event is not None:
                    await asyncio.to_thread(pause_event.wait)
                return ctx.next.complete("cancelled")
            return ctx.next.complete("unreachable")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _ndet_input_v1_worker(wf_type: str, pause_event=None) -> None:
    """Worker A: calls task with arg 'hello', then pauses. Killed before step completes."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task
        async def greet(value: str) -> str:
            return value.upper()

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            result = await greet(value="hello")
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete(result)

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _ndet_input_v2_worker(wf_type: str) -> None:
    """Worker B: calls same task with arg 'world' — diverges from history → NonDeterminismError."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task
        async def greet(value: str) -> str:
            return value.upper()

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            result = await greet(value="world")  # different input: diverges from history
            return ctx.next.complete(result)

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _ndet_v1_worker(wf_type: str, pause_event=None) -> None:
    """Worker A: executes task_a then pauses. Killed before task_b runs."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task
        async def task_a() -> str:
            return "a"

        @task
        async def task_b() -> str:
            return "b"

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            r = await task_a()
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            await task_b()
            return ctx.next.complete(r)

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _ndet_v2_worker(wf_type: str) -> None:
    """Worker B: executes task_b first — diverges from history → NonDeterminismError."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task
        async def task_a() -> str:
            return "a"

        @task
        async def task_b() -> str:
            return "b"

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            r = await task_b()  # Reversed order: task_b before task_a
            await task_a()
            return ctx.next.complete(r)

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


# ─── Tests ────────────────────────────────────────────────────────────────────


async def test_completed_task_is_skipped_on_step_retry(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_task_replay_skip")

    worker_a = multiprocessing.Process(target=_replay_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_replay_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        _, history_events = await history.wait_for_kind(HistoryKind.task_completed)
        _terminate(worker_a)
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=60.0)

        assert result == "done"

        task_events = [e for e in history_events if e.kind in _TASK_HISTORY_KINDS]
        started = [e for e in task_events if e.kind == HistoryKind.task_started]
        assert len(started) == 1
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_completed_task_result_is_preserved_on_step_retry(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_task_replay_result")

    worker_a = multiprocessing.Process(target=_replay_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_replay_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)

    try:
        await HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT).wait_for_kind(
            HistoryKind.task_completed
        )
        _terminate(worker_a)
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=60.0)

        assert result == "done"
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_failed_task_is_replayed_as_exception_on_step_retry(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_task_replay_exception")

    worker_a = multiprocessing.Process(target=_replay_exception_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_replay_exception_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        _, history_events = await history.wait_for_kind(HistoryKind.task_failed)
        _terminate(worker_a)
        worker_b.start()

        with pytest.raises(WorkflowError, match="ValueError: original error"):
            await asyncio.wait_for(handle.future, timeout=60.0)

        task_events = [e for e in history_events if e.kind in _TASK_HISTORY_KINDS]
        started = [e for e in task_events if e.kind == HistoryKind.task_started]
        attempt_failed = [e for e in task_events if e.kind == HistoryKind.task_attempt_failed]
        failed = [e for e in task_events if e.kind == HistoryKind.task_failed]
        assert len(started) == 1, "There should be one task started event"
        assert len(attempt_failed) == 1, "There should be one task attempt failed event"
        assert len(failed) == 1, "There should be one task failed event"
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_cancelled_task_is_replayed_as_cancelled_error_on_step_retry(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_task_replay_cancelled")

    worker_a = multiprocessing.Process(target=_replay_cancelled_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_replay_cancelled_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        _, history_events = await history.wait_for_kind(HistoryKind.task_cancelled)
        _terminate(worker_a)
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=60.0)

        assert result == "cancelled"

        task_events = [e for e in history_events if e.kind in _TASK_HISTORY_KINDS]
        started = [e for e in task_events if e.kind == HistoryKind.task_started]
        cancelled = [e for e in task_events if e.kind == HistoryKind.task_cancelled]
        assert len(started) == 1, "There should be one started event"
        assert len(cancelled) == 1, "There should be one cancelled event"
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_nondeterminism_raises_when_task_input_changes(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_task_replay_ndet_input")

    worker_a = multiprocessing.Process(target=_ndet_input_v1_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_ndet_input_v2_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf_type,
        id=wf_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    try:
        await HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT).wait_for_kind(
            HistoryKind.task_completed
        )
        _terminate(worker_a)
        worker_b.start()

        with pytest.raises(WorkflowError, match="NonDeterminism"):
            await asyncio.wait_for(handle.future, timeout=60.0)
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_nondeterminism_raises_when_operation_order_changes(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_task_replay_nondeterminism")

    worker_a = multiprocessing.Process(target=_ndet_v1_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_ndet_v2_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf_type,
        id=wf_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    try:
        await HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT).wait_for_kind(
            HistoryKind.task_completed
        )
        _terminate(worker_a)
        worker_b.start()

        with pytest.raises(WorkflowError, match="NonDeterminism"):
            await asyncio.wait_for(handle.future, timeout=60.0)
    finally:
        _terminate(worker_a)
        _terminate(worker_b)
