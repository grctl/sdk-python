import asyncio
import multiprocessing
import os
from datetime import timedelta

import ulid

from grctl.models import HistoryKind
from grctl.nats.connection import Connection
from grctl.worker import Context, task
from grctl.worker.worker import Worker
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess

_WORKER_INIT_DELAY = 0.5
_HISTORY_TIMEOUT = 15.0
_WORKFLOW_TIMEOUT = timedelta(seconds=120)
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


def _start_step_replay_worker(wf_type: str, pause_event=None) -> None:
    """Worker with a task inside the start step. Pauses after the task completes.

    Worker A holds the start directive unacked while paused — killing it simulates a crash.
    Worker B picks up the re-delivered start directive and replays the completed task from history.
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
            result = await simple_task()
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete(result)

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


# ─── Tests ────────────────────────────────────────────────────────────────────


async def test_completed_task_is_skipped_on_start_step_retry(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_start_replay_skip")

    worker_a = multiprocessing.Process(target=_start_step_replay_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_start_step_replay_worker, args=(wf_type,), daemon=True)
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
