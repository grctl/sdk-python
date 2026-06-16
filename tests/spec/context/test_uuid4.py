"""Spec tests: ctx.uuid4() deterministic replay."""

import asyncio
import multiprocessing
import os
import uuid
from datetime import timedelta
from typing import cast

import ulid

from grctl.models import HistoryKind
from grctl.models.history import UuidRecorded
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


def _terminate(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=5.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=1.0)


def _uuid4_replay_worker(wf_type: str, pause_event=None) -> None:
    async def run() -> None:
        os.environ.setdefault("ENGINE_NATS_WORKER_ACK_WAIT", _REPLAY_WORKER_ACK_WAIT_SECONDS)
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        wf = Workflow(workflow_type=wf_type)

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            value = await ctx.uuid4()
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete(str(value))

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.run()

    asyncio.run(run())


async def test_uuid4_returns_same_value_on_replay(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = unique_workflow_type("spec_ctx_det_uuid4_replay")

    worker_a = multiprocessing.Process(target=_uuid4_replay_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_uuid4_replay_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        recorded_event, _ = await history.wait_for_kind(HistoryKind.uuid_recorded)
        _terminate(worker_a)
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=30.0)

        assert result == cast("UuidRecorded", recorded_event.msg).value
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_uuid4_records_value_in_history(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_ctx_det_uuid4_hist"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        value = await ctx.uuid4()
        return ctx.next.complete(str(value))

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf.workflow_type, id=wf_id, input={}, timeout=timedelta(seconds=30))
    await asyncio.wait_for(handle.future, timeout=30.0)

    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
    event, _ = await history.wait_for_kind(HistoryKind.uuid_recorded)

    uuid.UUID(cast("UuidRecorded", event.msg).value)  # raises if not a valid UUID
