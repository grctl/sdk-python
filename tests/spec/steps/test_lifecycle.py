import asyncio
from datetime import timedelta

import ulid

from grctl.models import HistoryKind
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_echo_workflow, two_step_wf


async def test_step_emits_started_and_completed(worker, grctl_client) -> None:
    wf = make_echo_workflow(prefix="spec_steps_lifecycle_emits")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={"value": "ok"},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == "ok"

    step_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_step(
        [HistoryKind.step_started, HistoryKind.step_completed]
    )

    assert step_events[0].kind == HistoryKind.step_started
    assert step_events[1].kind == HistoryKind.step_completed


async def test_multi_step_workflow_emits_events_in_order(worker, grctl_client) -> None:
    await worker([two_step_wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=two_step_wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == "two-step-ok"

    step_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_step(
        [
            HistoryKind.step_started,
            HistoryKind.step_completed,
            HistoryKind.step_started,
            HistoryKind.step_completed,
        ]
    )

    assert step_events[0].msg.step_name == "start"  # ty:ignore[unresolved-attribute]
    assert step_events[1].msg.step_name == "start"  # ty:ignore[unresolved-attribute]
    assert step_events[2].msg.step_name == "two_step_second"  # ty:ignore[unresolved-attribute]
    assert step_events[3].msg.step_name == "two_step_second"  # ty:ignore[unresolved-attribute]


async def test_step_receives_task_result(worker, grctl_client) -> None:
    wf = make_echo_workflow(prefix="spec_steps_lifecycle_task_result")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={"value": "task-result-value"},
        timeout=timedelta(seconds=30),
    )

    result = await asyncio.wait_for(handle.future, timeout=30)

    assert result == "task-result-value"


async def test_completed_step_does_not_emit_timeout(worker, grctl_client) -> None:
    wf = make_echo_workflow(prefix="spec_steps_lifecycle_no_timeout")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={"value": "ok"},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == "ok"

    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
    await history.wait_for_step([HistoryKind.step_started, HistoryKind.step_completed])

    events = await history.events()
    assert not any(e.kind == HistoryKind.step_timeout for e in events)
