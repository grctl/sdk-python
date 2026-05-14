import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowError
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_blocking_step_workflow


async def test_step_timeout_emits_timeout_event(worker, grctl_client) -> None:
    wf = make_blocking_step_workflow(prefix="spec_steps_timeout_emits")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    step_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_step(
        [
            HistoryKind.step_started,
            HistoryKind.step_completed,
            HistoryKind.step_started,
            HistoryKind.step_timeout,
        ]
    )

    timeout_event = step_events[-1]
    assert timeout_event.kind == HistoryKind.step_timeout
    assert timeout_event.msg.step_name == "blocking_step"  # ty:ignore[unresolved-attribute]


async def test_step_timeout_fails_workflow(worker, grctl_client) -> None:
    wf = make_blocking_step_workflow(prefix="spec_steps_timeout_fails")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    with pytest.raises(WorkflowError):
        await asyncio.wait_for(handle.future, timeout=30)

    await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run(
        [HistoryKind.run_started, HistoryKind.run_failed]
    )
