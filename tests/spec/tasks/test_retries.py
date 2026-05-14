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


async def test_task_retries_on_failure_and_succeeds(worker, grctl_client) -> None:
    counter = {"value": 0}
    wf = Workflow(workflow_type="spec_task_retry_success")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def flaky() -> str:
        counter["value"] += 1
        if counter["value"] < 3:
            raise RuntimeError(f"transient attempt {counter['value']}")
        return "ok"

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await flaky()
        return ctx.next.complete(result)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == "ok"
    assert counter["value"] == 3


async def test_task_fails_after_exhausting_max_attempts(worker, grctl_client) -> None:
    counter = {"value": 0}
    wf = Workflow(workflow_type="spec_task_retry_exhausted")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def always_fail() -> str:
        counter["value"] += 1
        raise RuntimeError(f"attempt {counter['value']}")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await always_fail()
        return ctx.next.complete(result)

    await worker([wf])

    with pytest.raises(WorkflowError, match=r"RuntimeError: attempt 3"):
        await grctl_client.run_workflow(
            type=wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )

    assert counter["value"] == 3


async def test_task_without_retry_policy_fails_immediately(worker, grctl_client) -> None:
    counter = {"value": 0}
    wf = Workflow(workflow_type="spec_task_retry_no_policy")

    @task
    async def no_retry_task() -> str:
        counter["value"] += 1
        raise RuntimeError("fails immediately")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await no_retry_task()
        return ctx.next.complete(result)

    await worker([wf])

    with pytest.raises(WorkflowError, match=r"RuntimeError: fails immediately"):
        await grctl_client.run_workflow(
            type=wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )

    assert counter["value"] == 1


async def test_non_retryable_error_is_not_retried(worker, grctl_client) -> None:
    counter = {"value": 0}
    wf = Workflow(workflow_type="spec_task_retry_non_retryable")

    @task(
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay_ms=1,
            backoff_multiplier=1.0,
            non_retryable_errors=["ValueError"],
        )
    )
    async def non_retryable_task() -> str:
        counter["value"] += 1
        raise ValueError("not retryable")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await non_retryable_task()
        return ctx.next.complete(result)

    await worker([wf])

    with pytest.raises(WorkflowError, match=r"ValueError: not retryable"):
        await grctl_client.run_workflow(
            type=wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )

    assert counter["value"] == 1


async def test_retryable_errors_filter_only_retries_matching_type(worker, grctl_client) -> None:
    timeout_counter = {"value": 0}
    value_counter = {"value": 0}
    timeout_wf = Workflow(workflow_type="spec_task_retry_filter_matching")
    value_wf = Workflow(workflow_type="spec_task_retry_filter_non_matching")

    @task(
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay_ms=1,
            backoff_multiplier=1.0,
            retryable_errors=["TimeoutError"],
        )
    )
    async def timeout_task() -> str:
        timeout_counter["value"] += 1
        if timeout_counter["value"] < 3:
            raise TimeoutError("temporary timeout")
        return "ok"

    @timeout_wf.start()
    async def timeout_start(ctx: Context) -> Directive:
        result = await timeout_task()
        return ctx.next.complete(result)

    @task(
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay_ms=1,
            backoff_multiplier=1.0,
            retryable_errors=["TimeoutError"],
        )
    )
    async def value_task() -> str:
        value_counter["value"] += 1
        raise ValueError("wrong error type")

    @value_wf.start()
    async def value_start(ctx: Context) -> Directive:
        result = await value_task()
        return ctx.next.complete(result)

    await worker([timeout_wf, value_wf])

    result = await grctl_client.run_workflow(
        type=timeout_wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == "ok"
    assert timeout_counter["value"] == 3

    with pytest.raises(WorkflowError, match=r"ValueError: wrong error type"):
        await grctl_client.run_workflow(
            type=value_wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )

    assert value_counter["value"] == 1


async def test_cancelled_error_is_never_retried(worker, grctl_client) -> None:
    wf = Workflow(workflow_type="spec_task_retry_cancelled")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def cancelled_task() -> str:
        raise asyncio.CancelledError("cancel now")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await cancelled_task()
        return ctx.next.complete(result)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    task_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_task(
        [HistoryKind.task_started, HistoryKind.task_cancelled]
    )

    assert task_events[-1].kind == HistoryKind.task_cancelled
