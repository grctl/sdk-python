import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.directive import RetryPolicy
from grctl.models.errors import WorkflowError
from grctl.worker import Context, task
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess


@pytest.mark.asyncio
async def test_successful_task_emits_started_and_completed(worker, grctl_client) -> None:
    wf = Workflow(workflow_type="spec_task_lifecycle_success")

    @task
    async def succeed(value: str) -> str:
        return value

    @wf.start()
    async def start(ctx: Context, value: str) -> Directive:
        result = await succeed(value)
        return ctx.next.complete(result)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={"value": "ok"},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == "ok"

    task_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_task(
        [HistoryKind.task_started, HistoryKind.task_completed]
    )

    assert task_events[0].kind == HistoryKind.task_started
    assert task_events[1].kind == HistoryKind.task_completed


@pytest.mark.asyncio
async def test_retried_task_emits_attempt_failed_events(worker, grctl_client) -> None:
    wf = Workflow(workflow_type="spec_task_lifecycle_retry")
    call_count = 0

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def flaky() -> str:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError(f"attempt {call_count}")
        return "ok"

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await flaky()
        return ctx.next.complete(result)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == "ok"

    task_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_task(
        [
            HistoryKind.task_started,
            HistoryKind.task_attempt_failed,
            HistoryKind.task_attempt_failed,
            HistoryKind.task_completed,
        ]
    )

    assert task_events[1].msg.attempt == 1  # ty:ignore[unresolved-attribute]
    assert task_events[2].msg.attempt == 2  # ty:ignore[unresolved-attribute]


@pytest.mark.asyncio
async def test_failed_task_emits_started_and_failed(worker, grctl_client) -> None:
    wf = Workflow(workflow_type="spec_task_lifecycle_failed")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def always_fail() -> str:
        raise RuntimeError("permanent failure")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await always_fail()
        return ctx.next.complete(result)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    with pytest.raises(WorkflowError, match="RuntimeError: permanent failure"):
        await asyncio.wait_for(handle.future, timeout=30)

    task_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_task(
        [
            HistoryKind.task_started,
            HistoryKind.task_attempt_failed,
            HistoryKind.task_attempt_failed,
            HistoryKind.task_failed,
        ]
    )

    assert task_events[-1].kind == HistoryKind.task_failed
