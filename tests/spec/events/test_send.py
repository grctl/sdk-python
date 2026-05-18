import asyncio
from datetime import timedelta

import ulid

from grctl.models import HistoryKind
from grctl.models.history import EventReceived
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_waiting_event_workflow


async def test_send_event_triggers_event_handler(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_events_send_trigger")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_kind(HistoryKind.wait_started)
        await handle.send("finish", {"result": "approved"})

        result = await asyncio.wait_for(handle.future, timeout=30)
        assert result == "approved"
    finally:
        await handle.future.stop()


async def test_send_event_payload_is_received_by_handler(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_events_send_payload")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_kind(HistoryKind.wait_started)
        await handle.send("finish", {"result": "payload-val"})

        result = await asyncio.wait_for(handle.future, timeout=30)
        assert result == "payload-val"
    finally:
        await handle.future.stop()


async def test_send_event_emits_event_received_in_history(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_events_send_history")
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

        event, _ = await history.wait_for_kind(HistoryKind.event_received)
        assert isinstance(event.msg, EventReceived)
        assert event.msg.event_name == "finish"

        await asyncio.wait_for(handle.future, timeout=30)
    finally:
        await handle.future.stop()
