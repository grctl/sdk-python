import asyncio
import logging

from grctl.logging_config import get_logger, setup_logging
from grctl.models import (
    Directive,
    DirectiveKind,
    Start,
)
from grctl.models.directive import RetryPolicy
from grctl.models.history import (
    HistoryKind,
    TaskAttemptFailed,
    TaskCompleted,
    TaskFailed,
    TaskStarted,
)
from grctl.worker.context import Context
from grctl.worker.run_manager import RunManager
from grctl.worker.task import (
    _calculate_backoff_delay,
    _is_error_retryable,
    task,
)
from grctl.workflow import Workflow
from tests.conftest import create_directive

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


def _extract_history_events(published, manifest=None):
    return [msg for subject, msg in published if "grctl_history" in subject and not isinstance(msg, str)]


def _extract_task_events(published):
    events = _extract_history_events(published)
    return [e for e in events if isinstance(e.msg, (TaskStarted, TaskCompleted, TaskFailed, TaskAttemptFailed))]


# --- Test 1: Task fails once, succeeds on retry ---


async def test_task_retries_then_succeeds(mock_connection):
    connection, published = mock_connection
    call_count = 0

    wf = Workflow(workflow_type="RetryTest")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def flaky_task(name: str) -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient failure")
        return f"Hello, {name}!"

    @wf.start()
    async def start(ctx: Context, name: str) -> Directive:
        result = await flaky_task(name)
        ctx.store.put("result", result)
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={"name": "Test"}),
        directive_id="dir-1",
        run_id="run-1",
        wf_id="wf-1",
        wf_type="RetryTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    task_events = _extract_task_events(published)

    assert call_count == 2
    assert isinstance(task_events[0].msg, TaskStarted)
    assert isinstance(task_events[1].msg, TaskAttemptFailed)
    assert isinstance(task_events[2].msg, TaskCompleted)
    assert task_events[2].msg.output == {"result": "Hello, Test!"}


# --- Test 2: Task fails max_attempts times → raises ---


async def test_task_retries_exhausted(mock_connection):
    connection, published = mock_connection

    wf = Workflow(workflow_type="RetryExhaustTest")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def always_fails(name: str) -> str:
        raise RuntimeError("permanent failure")

    @wf.start()
    async def start(ctx: Context, name: str) -> Directive:
        result = await always_fails(name)
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={"name": "Test"}),
        directive_id="dir-2",
        run_id="run-2",
        wf_id="wf-2",
        wf_type="RetryExhaustTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    task_events = _extract_task_events(published)

    # 1 started + 2 attempt_failed + 1 task_failed
    assert isinstance(task_events[0].msg, TaskStarted)
    assert isinstance(task_events[1].msg, TaskAttemptFailed)
    assert isinstance(task_events[2].msg, TaskAttemptFailed)
    assert isinstance(task_events[3].msg, TaskFailed)
    assert task_events[3].msg.error.type == "RuntimeError"


# --- Test 3: Non-retryable error skips retries ---


async def test_non_retryable_error_skips_retry(mock_connection):
    connection, published = mock_connection
    call_count = 0

    wf = Workflow(workflow_type="NonRetryableTest")

    @task(
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay_ms=1,
            non_retryable_errors=["ValueError"],
        )
    )
    async def raises_value_error() -> str:
        nonlocal call_count
        call_count += 1
        raise ValueError("bad input")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await raises_value_error()
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={}),
        directive_id="dir-3",
        run_id="run-3",
        wf_id="wf-3",
        wf_type="NonRetryableTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    assert call_count == 1
    task_events = _extract_task_events(published)
    assert isinstance(task_events[-1].msg, TaskFailed)


# --- Test 4: retryable_errors set, wrong error → no retry ---


async def test_retryable_errors_wrong_type_no_retry(mock_connection):
    connection, _published = mock_connection
    call_count = 0

    wf = Workflow(workflow_type="RetryableWrongTest")

    @task(
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay_ms=1,
            retryable_errors=["TimeoutError"],
        )
    )
    async def raises_value_error() -> str:
        nonlocal call_count
        call_count += 1
        raise ValueError("not retryable")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await raises_value_error()
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={}),
        directive_id="dir-4",
        run_id="run-4",
        wf_id="wf-4",
        wf_type="RetryableWrongTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    assert call_count == 1


# --- Test 5: retryable_errors set, matching error → retries ---


async def test_retryable_errors_matching_type_retries(mock_connection):
    connection, published = mock_connection
    call_count = 0

    wf = Workflow(workflow_type="RetryableMatchTest")

    @task(
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay_ms=1,
            retryable_errors=["TimeoutError"],
        )
    )
    async def raises_timeout() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise TimeoutError("timed out")
        return "ok"

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await raises_timeout()
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={}),
        directive_id="dir-5",
        run_id="run-5",
        wf_id="wf-5",
        wf_type="RetryableMatchTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    assert call_count == 3
    task_events = _extract_task_events(published)
    assert isinstance(task_events[-1].msg, TaskCompleted)


# --- Test 6: CancelledError → no retry ---


async def test_cancelled_error_never_retried(mock_connection):
    connection, _published = mock_connection
    call_count = 0

    wf = Workflow(workflow_type="CancelTest")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1))
    async def cancellable_task() -> str:
        nonlocal call_count
        call_count += 1
        raise asyncio.CancelledError

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await cancellable_task()
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={}),
        directive_id="dir-6",
        run_id="run-6",
        wf_id="wf-6",
        wf_type="CancelTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    assert call_count == 1


# --- Test 7: Task without retry_policy fails → no retry ---


async def test_task_without_retry_policy_no_retry(mock_connection):
    connection, published = mock_connection
    call_count = 0

    wf = Workflow(workflow_type="NoRetryPolicyTest")

    @task
    async def no_retry_task() -> str:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("fails immediately")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await no_retry_task()
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={}),
        directive_id="dir-7",
        run_id="run-7",
        wf_id="wf-7",
        wf_type="NoRetryPolicyTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    assert call_count == 1
    task_events = _extract_task_events(published)
    assert isinstance(task_events[-1].msg, TaskFailed)


# --- Test 8: 2x attempt_failed + 1x completed ---


async def test_history_events_attempt_failed_then_completed(mock_connection):
    connection, published = mock_connection
    call_count = 0

    wf = Workflow(workflow_type="HistoryRetryTest")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def fails_twice(name: str) -> str:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError(f"attempt {call_count}")
        return f"Hello, {name}!"

    @wf.start()
    async def start(ctx: Context, name: str) -> Directive:
        result = await fails_twice(name)
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={"name": "Test"}),
        directive_id="dir-8",
        run_id="run-8",
        wf_id="wf-8",
        wf_type="HistoryRetryTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    task_events = _extract_task_events(published)

    assert isinstance(task_events[0].msg, TaskStarted)
    assert isinstance(task_events[1].msg, TaskAttemptFailed)
    assert task_events[1].msg.attempt == 1
    assert task_events[1].msg.max_attempts == 3
    assert isinstance(task_events[2].msg, TaskAttemptFailed)
    assert task_events[2].msg.attempt == 2
    assert isinstance(task_events[3].msg, TaskCompleted)


# --- Test 9: Nx attempt_failed + 1x failed (terminal) ---


async def test_history_events_all_attempts_failed(mock_connection):
    connection, published = mock_connection

    wf = Workflow(workflow_type="HistoryExhaustTest")

    @task(retry_policy=RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    async def always_fails() -> str:
        raise RuntimeError("permanent")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await always_fails()
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={}),
        directive_id="dir-9",
        run_id="run-9",
        wf_id="wf-9",
        wf_type="HistoryExhaustTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    task_events = _extract_task_events(published)

    assert isinstance(task_events[0].msg, TaskStarted)
    assert isinstance(task_events[1].msg, TaskAttemptFailed)
    assert isinstance(task_events[2].msg, TaskAttemptFailed)
    assert isinstance(task_events[3].msg, TaskFailed)
    # Terminal event is task.failed, not attempt_failed
    assert task_events[3].kind == HistoryKind.task_failed


# --- Test 10: Backoff delay doubles ---


def test_backoff_delay_doubles():
    policy = RetryPolicy(initial_delay_ms=100, backoff_multiplier=2.0, max_delay_ms=5000)

    delay_1 = _calculate_backoff_delay(policy, 1)
    delay_2 = _calculate_backoff_delay(policy, 2)
    delay_3 = _calculate_backoff_delay(policy, 3)

    assert delay_1 == 100
    assert delay_2 == 200
    assert delay_3 == 400


def test_backoff_delay_respects_max():
    policy = RetryPolicy(initial_delay_ms=100, backoff_multiplier=10.0, max_delay_ms=500)

    delay = _calculate_backoff_delay(policy, 3)
    assert delay == 500


# --- Test 11: @task with no arguments still works ---


async def test_bare_task_decorator(mock_connection):
    connection, published = mock_connection

    wf = Workflow(workflow_type="BareDecoratorTest")

    @task
    async def simple_task(name: str) -> str:
        return f"Hello, {name}!"

    @wf.start()
    async def start(ctx: Context, name: str) -> Directive:
        result = await simple_task(name)
        return ctx.next.complete(result)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(input={"name": "Test"}),
        directive_id="dir-11",
        run_id="run-11",
        wf_id="wf-11",
        wf_type="BareDecoratorTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    task_events = _extract_task_events(published)
    assert isinstance(task_events[0].msg, TaskStarted)
    assert isinstance(task_events[1].msg, TaskCompleted)
    assert task_events[1].msg.output == {"result": "Hello, Test!"}


# --- Unit tests for _is_error_retryable ---


def test_is_error_retryable_no_filters():
    policy = RetryPolicy(max_attempts=3)
    assert _is_error_retryable(RuntimeError("x"), policy) is True


def test_is_error_retryable_in_retryable_list():
    policy = RetryPolicy(max_attempts=3, retryable_errors=["TimeoutError"])
    assert _is_error_retryable(TimeoutError("x"), policy) is True
    assert _is_error_retryable(ValueError("x"), policy) is False


def test_is_error_retryable_in_non_retryable_list():
    policy = RetryPolicy(max_attempts=3, non_retryable_errors=["ValueError"])
    assert _is_error_retryable(ValueError("x"), policy) is False
    assert _is_error_retryable(RuntimeError("x"), policy) is True


def test_is_error_retryable_both_lists():
    policy = RetryPolicy(
        max_attempts=3,
        retryable_errors=["RuntimeError", "TimeoutError"],
        non_retryable_errors=["TimeoutError"],
    )
    assert _is_error_retryable(RuntimeError("x"), policy) is True
    assert _is_error_retryable(TimeoutError("x"), policy) is False
    assert _is_error_retryable(ValueError("x"), policy) is False
