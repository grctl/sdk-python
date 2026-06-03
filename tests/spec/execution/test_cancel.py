import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_slow_step_workflow, make_waiting_event_workflow


async def test_cancel_during_step_emits_cancel_received(worker, grctl_client) -> None:
    wf = make_slow_step_workflow(
        step_sleep=2.0, step_timeout=timedelta(seconds=30), prefix="spec_cancel_inflight_received"
    )
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=15.0)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started])

    await handle.cancel(reason="test cancel during step")

    await history.wait_for_kind(HistoryKind.run_cancel_received)
    await handle.future.discard()


async def test_cancel_during_step_allows_step_to_complete(worker, grctl_client) -> None:
    wf = make_slow_step_workflow(
        step_sleep=2.0, step_timeout=timedelta(seconds=30), prefix="spec_cancel_inflight_step_complete"
    )
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=15.0)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started])

    await handle.cancel(reason="test cancel during step")

    await history.wait_for_kind(HistoryKind.run_cancelled)

    events = await history.events()
    step_kinds = [e.kind for e in events if e.kind in (HistoryKind.step_started, HistoryKind.step_completed)]
    assert step_kinds == [
        HistoryKind.step_started,
        HistoryKind.step_completed,
        HistoryKind.step_started,
        HistoryKind.step_completed,
    ]
    await handle.future.discard()


async def test_cancel_during_step_transitions_run_to_cancelled(worker, grctl_client) -> None:
    wf = make_slow_step_workflow(
        step_sleep=2.0, step_timeout=timedelta(seconds=30), prefix="spec_cancel_inflight_run_cancelled"
    )
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=15.0)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started])

    await handle.cancel(reason="test cancel during step")

    _, all_events = await history.wait_for_kind(HistoryKind.run_cancelled)

    kinds = [e.kind for e in all_events]
    cancel_received_idx = kinds.index(HistoryKind.run_cancel_received)
    step_completed_idx = kinds.index(HistoryKind.step_completed, cancel_received_idx)
    run_cancelled_idx = kinds.index(HistoryKind.run_cancelled)
    assert cancel_received_idx < step_completed_idx < run_cancelled_idx
    await handle.future.discard()


async def test_cancel_during_step_future_raises_cancelled_error(worker, grctl_client) -> None:
    wf = make_slow_step_workflow(
        step_sleep=2.0, step_timeout=timedelta(seconds=30), prefix="spec_cancel_inflight_future"
    )
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=15.0)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed, HistoryKind.step_started])

    await handle.cancel(reason="test cancel during step")

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(handle.future, timeout=15)


async def test_cancel_while_waiting_no_cancel_received(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_cancel_wait_no_received")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=10.0)
    await history.wait_for_kind(HistoryKind.wait_started)

    await handle.cancel(reason="test cancel while waiting")

    _, all_events = await history.wait_for_kind(HistoryKind.run_cancelled)

    kinds = [e.kind for e in all_events]
    assert HistoryKind.run_cancel_received not in kinds
    assert HistoryKind.run_cancelled in kinds
    await handle.future.discard()
