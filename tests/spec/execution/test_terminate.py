import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_blocking_step_workflow


async def test_terminate_while_step_in_flight(worker, grctl_client) -> None:
    wf = make_blocking_step_workflow(step_timeout=timedelta(seconds=30), prefix="spec_terminate_inflight")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=10.0)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started])

    await handle.terminate(reason="test termination")

    await history.wait_for_kind(HistoryKind.run_terminated)
    await handle.future.discard()


async def test_terminated_step_emits_no_step_completed(worker, grctl_client) -> None:
    wf = make_blocking_step_workflow(step_timeout=timedelta(seconds=30), prefix="spec_terminate_no_step_completed")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=10.0)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started])

    await handle.terminate(reason="test termination")

    await history.wait_for_kind(HistoryKind.run_terminated)

    events = await history.events()
    step_kinds = [
        e.kind
        for e in events
        if e.kind in (HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_failed)
    ]
    assert step_kinds == [HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started]
    await handle.future.discard()


async def test_terminate_future_raises(worker, grctl_client) -> None:
    wf = make_blocking_step_workflow(step_timeout=timedelta(seconds=30), prefix="spec_terminate_future_raises")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=10.0)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started])

    await handle.terminate(reason="test termination")

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(handle.future, timeout=15)
