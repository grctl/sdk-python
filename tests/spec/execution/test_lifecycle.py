import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowAlreadyRunningError, WorkflowError, WorkflowNotFoundError
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_completing_workflow, make_failing_workflow, make_waiting_event_workflow


async def test_start_workflow_returns_handle_with_run_info(worker, grctl_client) -> None:
    wf = make_completing_workflow(prefix="spec_execution_start_handle")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        assert handle.run_info.wf_id == wf_id
        assert handle.run_info.wf_type == wf.workflow_type
        assert handle.run_info.id
    finally:
        await handle.future.stop()


async def test_workflow_future_resolves_with_result(worker, grctl_client) -> None:
    wf = make_completing_workflow(prefix="spec_execution_completion_result")
    await worker([wf])

    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == "ok"


async def test_run_workflow_returns_result(worker, grctl_client) -> None:
    wf = make_completing_workflow(prefix="spec_execution_run_result")
    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == "ok"


async def test_start_workflow_future_raises_workflow_error_on_failure(worker, grctl_client) -> None:
    wf = make_failing_workflow(message="workflow exploded", prefix="spec_execution_start_failure")
    await worker([wf])

    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    with pytest.raises(WorkflowError, match="ValueError: workflow exploded"):
        await asyncio.wait_for(handle.future, timeout=30)


async def test_run_workflow_raises_workflow_error_on_failure(worker, grctl_client) -> None:
    wf = make_failing_workflow(message="workflow exploded", prefix="spec_execution_run_failure")
    await worker([wf])

    with pytest.raises(WorkflowError, match="ValueError: workflow exploded"):
        await grctl_client.run_workflow(
            type=wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )


async def test_workflow_future_raises_timeout_on_workflow_timeout(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_execution_timeout")
    await worker([wf])

    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=2),
    )

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(handle.future, timeout=15)


async def test_start_workflow_raises_when_wf_id_already_active(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_execution_duplicate")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run([HistoryKind.run_started])

    try:
        with pytest.raises(WorkflowAlreadyRunningError):
            await grctl_client.start_workflow(
                type=wf.workflow_type,
                id=wf_id,
                input={},
                timeout=timedelta(seconds=30),
            )
    finally:
        await handle.future.stop()


async def test_cancel_workflow_future_raises_cancelled_error(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_execution_cancel_error")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run([HistoryKind.run_started])

    await handle.cancel(reason="test cancellation")

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(handle.future, timeout=15)


async def test_cancel_workflow_emits_history_events(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_execution_cancel_history")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
    await history.wait_for_run([HistoryKind.run_started])

    await handle.cancel()

    await history.wait_for_kind(HistoryKind.run_cancelled)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(handle.future, timeout=5)


async def test_get_workflow_handle_raises_not_found_for_unknown_wf_id(grctl_client) -> None:
    with pytest.raises(WorkflowNotFoundError):
        await grctl_client.get_workflow_handle(str(ulid.ULID()))


async def test_in_flight_workflow_receives_completion(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_events_lifecycle_inflight")
    await worker([wf])

    wf_id = str(ulid.ULID())
    original_handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    await HistoryAccess(grctl_client, wf_id, original_handle.run_info.id).wait_for_run([HistoryKind.run_started])

    attached_handle = await grctl_client.get_workflow_handle(wf_id)

    try:
        assert attached_handle.run_info.id == original_handle.run_info.id
        assert attached_handle.run_info.wf_id == wf_id
        assert attached_handle.run_info.wf_type == wf.workflow_type

        await attached_handle.send("finish", {"result": "attached-ok"})

        result = await asyncio.wait_for(attached_handle.future, timeout=30)
        assert result == "attached-ok"
        assert await asyncio.wait_for(original_handle.future, timeout=5) == "attached-ok"
    finally:
        await attached_handle.future.stop()
        await original_handle.future.stop()


async def test_in_flight_completed_workflow_resolves_future(worker, grctl_client) -> None:
    wf = make_completing_workflow(result="done", prefix="spec_events_lifecycle_completed")
    await worker([wf])

    wf_id = str(ulid.ULID())
    original_handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    await HistoryAccess(grctl_client, wf_id, original_handle.run_info.id).wait_for_kind(HistoryKind.run_completed)
    await original_handle.future.stop()

    attached_handle = await grctl_client.get_workflow_handle(wf_id)
    try:
        result = await asyncio.wait_for(attached_handle.future, timeout=10)
        assert result == "done"
    finally:
        await attached_handle.future.stop()
