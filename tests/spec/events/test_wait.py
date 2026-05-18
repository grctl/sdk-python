import asyncio
from datetime import timedelta

import ulid

from grctl.models import HistoryKind
from grctl.models.history import EventReceived
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_waiting_event_workflow


async def test_wait_emits_wait_started(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_event_wait_emits")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        event, _ = await history.wait_for_kind(HistoryKind.wait_started)
        assert event.kind == HistoryKind.wait_started
    finally:
        await handle.future.stop()


async def test_workflow_resumes_after_event_received(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_event_wait_resume")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        await history.wait_for_kind(HistoryKind.wait_started)
        await handle.send("finish")

        event, all_events = await history.wait_for_kind(HistoryKind.event_received)
        assert isinstance(event.msg, EventReceived)
        assert event.msg.event_name == "finish"

        kinds = [e.kind for e in all_events]
        wait_idx = kinds.index(HistoryKind.wait_started)
        recv_idx = kinds.index(HistoryKind.event_received)
        assert wait_idx < recv_idx

        result = await asyncio.wait_for(handle.future, timeout=30)
        assert result == "done"
    finally:
        await handle.future.stop()
