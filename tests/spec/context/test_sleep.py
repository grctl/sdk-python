"""Spec tests: ctx.sleep() deterministic replay."""

import asyncio
import multiprocessing
import os
from datetime import timedelta
from typing import cast

import ulid

from grctl.models import HistoryKind
from grctl.models.history import SleepRecorded
from grctl.nats.connection import Connection
from grctl.worker import Context
from grctl.worker.worker import Worker
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess
from tests.spec.workflows import unique_workflow_type

_WORKER_INIT_DELAY = 0.5
_HISTORY_TIMEOUT = 15.0
_WORKFLOW_TIMEOUT = timedelta(seconds=120)
_REPLAY_WORKER_ACK_WAIT_SECONDS = "0.5"

# Sleep duration long enough to detect if replay fails to skip it.
_SLEEP_DURATION = timedelta(seconds=5)
_SLEEP_DURATION_MS = int(_SLEEP_DURATION.total_seconds() * 1000)

# Replay must complete well under the sleep duration.
_REPLAY_TIMEOUT = 3.0


def _terminate(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=5.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=1.0)


def _sleep_replay_worker(wf_type: str, pause_event=None) -> None:
    async def run() -> None:
        os.environ.setdefault("ENGINE_NATS_WORKER_ACK_WAIT", _REPLAY_WORKER_ACK_WAIT_SECONDS)
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        wf = Workflow(workflow_type=wf_type)

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            await ctx.sleep(_SLEEP_DURATION)
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete("done")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


async def test_sleep_is_skipped_on_replay(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = unique_workflow_type("spec_ctx_det_sleep_replay")

    worker_a = multiprocessing.Process(target=_sleep_replay_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_sleep_replay_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        await history.wait_for_kind(HistoryKind.sleep_recorded)
        _terminate(worker_a)
        worker_b.start()
        await asyncio.sleep(_WORKER_INIT_DELAY)

        # If sleep is not skipped on replay, this will timeout before the workflow completes.
        result = await asyncio.wait_for(handle.future, timeout=_REPLAY_TIMEOUT)

        assert result == "done"
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_sleep_records_duration_in_history(worker, grctl_client) -> None:
    sleep_duration = timedelta(milliseconds=100)
    expected_duration_ms = 100

    wf = Workflow(workflow_type=unique_workflow_type("spec_ctx_det_sleep_hist"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        await ctx.sleep(sleep_duration)
        return ctx.next.complete("done")

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf.workflow_type, id=wf_id, input={}, timeout=timedelta(seconds=30))
    await asyncio.wait_for(handle.future, timeout=30.0)

    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
    event, _ = await history.wait_for_kind(HistoryKind.sleep_recorded)

    assert cast("SleepRecorded", event.msg).duration_ms == expected_duration_ms
