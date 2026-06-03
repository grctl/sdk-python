import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowError
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_failing_workflow


async def test_step_failure_emits_step_failed(worker, grctl_client) -> None:
    wf = make_failing_workflow(message="step exploded: code=42", prefix="spec_steps_failure_emits")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    step_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_step(
        [HistoryKind.step_started, HistoryKind.step_failed]
    )

    failed_event = step_events[-1]
    assert failed_event.kind == HistoryKind.step_failed
    assert failed_event.msg.error.type == "ValueError"  # ty:ignore[unresolved-attribute]
    assert "step exploded: code=42" in failed_event.msg.error.message  # ty:ignore[unresolved-attribute]
    await handle.future.discard()


async def test_step_failure_propagates_to_workflow_failure(worker, grctl_client) -> None:
    wf = make_failing_workflow(message="step exploded: code=42", prefix="spec_steps_failure_propagates")
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

    run_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run(
        [HistoryKind.run_started, HistoryKind.run_failed]
    )

    assert run_events[-1].kind == HistoryKind.run_failed


async def test_step_failure_message_is_preserved(worker, grctl_client) -> None:
    wf = make_failing_workflow(message="step exploded: code=42", prefix="spec_steps_failure_message")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    step_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_step(
        [HistoryKind.step_started, HistoryKind.step_failed]
    )

    failed_event = step_events[-1]
    assert "step exploded: code=42" in failed_event.msg.error.message  # ty:ignore[unresolved-attribute]
    await handle.future.discard()
