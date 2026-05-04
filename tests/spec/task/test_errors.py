import asyncio
import time
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryEvent, HistoryKind
from grctl.models.directive import RetryPolicy
from grctl.models.errors import WorkflowError
from grctl.worker import Context, task
from grctl.workflow import Directive, Workflow

_POLL_INTERVAL_SECONDS = 0.1
_HISTORY_TIMEOUT_SECONDS = 5.0

_TASK_HISTORY_KINDS = {
    HistoryKind.task_started,
    HistoryKind.task_completed,
    HistoryKind.task_attempt_failed,
    HistoryKind.task_failed,
    HistoryKind.task_cancelled,
}


async def _wait_for_task_history(
    grctl_client,
    wf_id: str,
    run_id: str,
    expected_kinds: list[HistoryKind],
) -> list[HistoryEvent]:
    deadline = time.monotonic() + _HISTORY_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        events = await grctl_client.get_history(wf_id, run_id=run_id)
        task_events = [event for event in events if event.kind in _TASK_HISTORY_KINDS]
        actual_kinds = [event.kind for event in task_events]
        if actual_kinds == expected_kinds:
            return task_events
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    raise AssertionError(f"Timed out waiting for task history {expected_kinds!r} for wf_id={wf_id} run_id={run_id}")


def _build_retry_exhausted_workflow() -> tuple[Workflow, dict[str, int]]:
    counter = {"value": 0}
    wf = Workflow(workflow_type="spec_task_error_retry_exhausted")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1))
    async def retry_exhausted_task() -> str:
        counter["value"] += 1
        raise RuntimeError(f"retry exhausted attempt {counter['value']}")

    @wf.start()
    async def retry_exhausted_start(ctx: Context) -> Directive:
        result = await retry_exhausted_task()
        return ctx.next.complete(result)

    return wf, counter


def _build_non_retryable_workflow() -> tuple[Workflow, dict[str, int]]:
    counter = {"value": 0}
    wf = Workflow(workflow_type="spec_task_error_non_retryable")

    @task(
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay_ms=1,
            backoff_multiplier=1.0,
            non_retryable_errors=["ValueError"],
        )
    )
    async def non_retryable_failure_task() -> str:
        counter["value"] += 1
        raise ValueError("fail immediately")

    @wf.start()
    async def non_retryable_start(ctx: Context) -> Directive:
        result = await non_retryable_failure_task()
        return ctx.next.complete(result)

    return wf, counter


def _build_basic_error_workflow() -> Workflow:
    wf = Workflow(workflow_type="spec_task_error_basic")

    @task
    async def basic_failure_task() -> str:
        raise ValueError("task exploded: code=123")

    @wf.start()
    async def basic_error_start(ctx: Context) -> Directive:
        result = await basic_failure_task()
        return ctx.next.complete(result)

    return wf


def _build_cancelled_workflow() -> Workflow:
    wf = Workflow(workflow_type="spec_task_error_cancelled")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1))
    async def cancelled_task() -> str:
        raise asyncio.CancelledError("cancel now")

    @wf.start()
    async def cancelled_start(ctx: Context) -> Directive:
        result = await cancelled_task()
        return ctx.next.complete(result)

    return wf


async def test_task_failure_propagates_to_step_failure(worker, grctl_client) -> None:
    basic_error_wf = _build_basic_error_workflow()
    await worker([basic_error_wf])

    with pytest.raises(WorkflowError):
        await grctl_client.run_workflow(
            type=basic_error_wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )


async def test_task_exception_message_is_preserved_on_failure(worker, grctl_client) -> None:
    basic_error_wf = _build_basic_error_workflow()
    await worker([basic_error_wf])

    with pytest.raises(WorkflowError, match="task exploded: code=123"):
        await grctl_client.run_workflow(
            type=basic_error_wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )


async def test_task_failure_preserves_exception_type_in_client_error(worker, grctl_client) -> None:
    basic_error_wf = _build_basic_error_workflow()
    await worker([basic_error_wf])

    with pytest.raises(WorkflowError, match=r"ValueError: task exploded: code=123"):
        await grctl_client.run_workflow(
            type=basic_error_wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )


async def test_task_failure_after_retries_still_propagates_to_step_failure(worker, grctl_client) -> None:
    retry_exhausted_wf, retry_exhausted_counter = _build_retry_exhausted_workflow()
    await worker([retry_exhausted_wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=retry_exhausted_wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    with pytest.raises(WorkflowError, match=r"RuntimeError: retry exhausted attempt 3"):
        await asyncio.wait_for(handle.future, timeout=2)

    assert retry_exhausted_counter["value"] == 3

    task_events = await _wait_for_task_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [
            HistoryKind.task_started,
            HistoryKind.task_attempt_failed,
            HistoryKind.task_attempt_failed,
            HistoryKind.task_failed,
        ],
    )

    assert task_events[1].msg.attempt == 1  # ty:ignore[unresolved-attribute]
    assert task_events[2].msg.attempt == 2  # ty:ignore[unresolved-attribute]


async def test_non_retryable_task_error_fails_step_immediately(worker, grctl_client) -> None:
    non_retryable_wf, non_retryable_counter = _build_non_retryable_workflow()
    await worker([non_retryable_wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=non_retryable_wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    with pytest.raises(WorkflowError, match=r"ValueError: fail immediately"):
        await asyncio.wait_for(handle.future, timeout=30)

    assert non_retryable_counter["value"] == 1

    task_events = await _wait_for_task_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [HistoryKind.task_started, HistoryKind.task_failed],
    )

    assert task_events[-1].kind == HistoryKind.task_failed


async def test_cancelled_task_error_propagates_without_retry(worker, grctl_client) -> None:
    cancelled_wf = _build_cancelled_workflow()
    await worker([cancelled_wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=cancelled_wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    task_events = await _wait_for_task_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [HistoryKind.task_started, HistoryKind.task_cancelled],
    )

    assert task_events[-1].kind == HistoryKind.task_cancelled
