"""Retry behaviour of `@task`.

Two levels are covered: `RetryRunner` on its own, which is where the decision to retry
lives, and a task run through a real journal, which is where those decisions become
durable history.
"""

from __future__ import annotations

import asyncio

import msgspec
import pytest
from pydantic import BaseModel

from grctl.exec.task import AttemptFailed, Cancelled, Failed, RetryRunner, Succeeded, _backoff_delay_ms, task
from grctl.models import HistoryKind
from grctl.models.directive import RetryPolicy
from grctl.models.history import TaskAttemptFailed, TaskCompleted, TaskStarted
from tests.unit.exec.fakes import DEFAULT_RUN_INFO, DEFAULT_WORKER_ID, FakeAppender, make_context

_IMMEDIATE = RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0)


class PydanticPayload(BaseModel):
    name: str
    count: int
    tags: list[str]


class StructPayload(msgspec.Struct):
    name: str
    count: int
    tags: list[str]


async def _collect(runner: RetryRunner) -> tuple[Succeeded | Failed | Cancelled, list[AttemptFailed]]:
    """Run a runner, returning its terminal outcome alongside the attempts it reported."""
    failures: list[AttemptFailed] = []

    async def on_attempt_failed(event: AttemptFailed) -> None:
        failures.append(event)

    outcome = await runner.run((), {}, on_attempt_failed)
    return outcome, failures


async def test_succeeds_on_first_attempt_without_reporting_failures() -> None:
    async def fn() -> str:
        return "ok"

    outcome, failures = await _collect(RetryRunner(fn, _IMMEDIATE))

    assert outcome == Succeeded(result="ok", attempts=1)
    assert failures == []


async def test_retries_until_the_call_succeeds() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient")
        return "ok"

    outcome, failures = await _collect(RetryRunner(fn, _IMMEDIATE))

    assert outcome == Succeeded(result="ok", attempts=3)
    assert [f.attempt for f in failures] == [1, 2]


async def test_max_attempts_counts_calls_not_retries() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("permanent")

    outcome, failures = await _collect(RetryRunner(fn, _IMMEDIATE))

    assert calls == 3
    assert isinstance(outcome, Failed)
    assert outcome.attempts == 3
    # The final attempt is reported as the terminal failure, not as a retryable one.
    assert [f.attempt for f in failures] == [1, 2]


async def test_no_policy_means_a_single_attempt() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("fail")

    outcome, failures = await _collect(RetryRunner(fn, None))

    assert calls == 1
    assert isinstance(outcome, Failed)
    assert failures == []


async def test_non_retryable_error_is_not_retried() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("bad input")

    policy = RetryPolicy(max_attempts=3, initial_delay_ms=1, non_retryable_errors=["ValueError"])
    outcome, failures = await _collect(RetryRunner(fn, policy))

    assert calls == 1
    assert isinstance(outcome, Failed)
    assert failures == []


async def test_error_outside_the_retryable_list_is_not_retried() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("not listed")

    policy = RetryPolicy(max_attempts=3, initial_delay_ms=1, retryable_errors=["TimeoutError"])
    outcome, _ = await _collect(RetryRunner(fn, policy))

    assert calls == 1
    assert isinstance(outcome, Failed)


async def test_error_inside_the_retryable_list_is_retried() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError("timed out")
        return "ok"

    policy = RetryPolicy(max_attempts=3, initial_delay_ms=1, retryable_errors=["TimeoutError"])
    outcome, _ = await _collect(RetryRunner(fn, policy))

    assert calls == 3
    assert isinstance(outcome, Succeeded)


async def test_cancellation_is_never_retried() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError

    outcome, failures = await _collect(RetryRunner(fn, _IMMEDIATE))

    assert calls == 1
    assert outcome == Cancelled(attempts=1)
    assert failures == []


async def test_attempts_already_made_count_against_the_budget() -> None:
    """A step re-delivered mid-task resumes the budget instead of starting a new one."""
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("permanent")

    outcome, failures = await _collect(RetryRunner(fn, _IMMEDIATE, already_attempted=2))

    assert calls == 1
    assert isinstance(outcome, Failed)
    assert outcome.attempts == 3
    assert failures == []


async def test_reported_delay_matches_the_backoff_schedule() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient")
        return "ok"

    policy = RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=2.0)
    _, failures = await _collect(RetryRunner(fn, policy))

    assert [f.delay_ms for f in failures] == [_backoff_delay_ms(policy, 1), _backoff_delay_ms(policy, 2)]


def test_backoff_is_capped_at_max_delay() -> None:
    policy = RetryPolicy(initial_delay_ms=100, backoff_multiplier=10.0, max_delay_ms=500)

    assert [_backoff_delay_ms(policy, n) for n in (1, 2, 3)] == [100, 500, 500]


# --- The task as a journal operation: what a retried task leaves in history ---


async def test_retried_task_records_started_attempts_and_one_terminal_entry(in_step) -> None:
    calls = 0

    @task(retry_policy=_IMMEDIATE)
    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError(f"attempt {calls}")
        return "ok"

    appender = FakeAppender()
    in_step(make_context(appender=appender))

    assert await flaky() == "ok"
    assert [e.kind for e in appender.events] == [
        HistoryKind.task_started,
        HistoryKind.task_attempt_failed,
        HistoryKind.task_attempt_failed,
        HistoryKind.task_completed,
    ]


async def test_history_entries_carry_the_operation_id_and_step(in_step) -> None:
    @task(retry_policy=_IMMEDIATE)
    async def flaky(value: int) -> int:
        raise RuntimeError("always")

    appender = FakeAppender()
    in_step(make_context(appender=appender))

    with pytest.raises(RuntimeError, match="always"):
        await flaky(7)

    started = appender.events[0]
    assert isinstance(started.msg, TaskStarted)
    # Name and call position stay readable; the argument fingerprint follows them.
    assert started.msg.task_id == started.operation_id
    assert started.operation_id.startswith("flaky:1:")
    assert started.msg.step_name == "current_step"
    assert started.msg.args == {"value": 7}
    assert started.wf_id == DEFAULT_RUN_INFO.wf_id
    assert started.worker_id == DEFAULT_WORKER_ID

    attempt = appender.events[1]
    assert isinstance(attempt.msg, TaskAttemptFailed)
    assert (attempt.msg.attempt, attempt.msg.max_attempts) == (1, 3)
    assert attempt.msg.error.type == "RuntimeError"
    assert attempt.msg.error.qualified_type == "builtins.RuntimeError"


async def test_replay_resolves_on_the_terminal_entry_and_reruns_nothing() -> None:
    calls = 0

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 2:
            raise RuntimeError("transient")
        return "ok"

    appender = FakeAppender()
    ctx = make_context(appender=appender)
    await ctx.run_task(flaky, (), {}, _IMMEDIATE)
    assert calls == 2

    replay_appender = FakeAppender()
    replay_ctx = make_context(appender.events, appender=replay_appender)

    assert await replay_ctx.run_task(flaky, (), {}, _IMMEDIATE) == "ok"
    assert calls == 2  # the recorded attempts are not re-run
    assert replay_appender.events == []


async def test_attempts_recorded_by_an_interrupted_execution_limit_the_retry() -> None:
    """Redelivery after a crash mid-task resumes the budget rather than restarting it."""
    calls = 0

    async def always_fails() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("permanent")

    first = FakeAppender()
    ctx = make_context(appender=first)
    with pytest.raises(RuntimeError):
        await ctx.run_task(always_fails, (), {}, _IMMEDIATE)
    assert calls == 3

    # History as it would look had the worker died after the first attempt: the task has
    # no terminal entry, so the re-delivered step runs it again — with 2 attempts left.
    partial = [e for e in first.events if e.kind != HistoryKind.task_failed][:2]
    calls = 0
    resumed_appender = FakeAppender()
    resumed_ctx = make_context(partial, appender=resumed_appender)

    with pytest.raises(RuntimeError):
        await resumed_ctx.run_task(always_fails, (), {}, _IMMEDIATE)

    assert calls == 2
    assert [e.kind for e in resumed_appender.events] == [
        HistoryKind.task_started,
        HistoryKind.task_attempt_failed,
        HistoryKind.task_failed,
    ]


async def test_cancelled_task_records_cancellation_and_raises_it_on_replay() -> None:
    async def cancels() -> None:
        raise asyncio.CancelledError

    appender = FakeAppender()
    ctx = make_context(appender=appender)

    with pytest.raises(asyncio.CancelledError):
        await ctx.run_task(cancels, (), {}, _IMMEDIATE)

    assert [e.kind for e in appender.events] == [HistoryKind.task_started, HistoryKind.task_cancelled]

    replay_ctx = make_context(appender.events, appender=FakeAppender())
    with pytest.raises(asyncio.CancelledError):
        await replay_ctx.run_task(cancels, (), {}, _IMMEDIATE)


async def test_bare_task_decorator_still_runs_once(in_step) -> None:
    calls = 0

    @task
    async def once() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("fail")

    appender = FakeAppender()
    in_step(make_context(appender=appender))

    with pytest.raises(RuntimeError, match="fail"):
        await once()

    assert calls == 1
    assert [e.kind for e in appender.events] == [HistoryKind.task_started, HistoryKind.task_failed]


async def test_pydantic_task_result_is_typed_from_its_recorded_outcome() -> None:
    payload = PydanticPayload(name="pydantic", count=7, tags=["a", "b"])
    calls = 0

    async def build_payload() -> PydanticPayload:
        nonlocal calls
        calls += 1
        return payload

    appender = FakeAppender()
    recorded = await make_context(appender=appender).run_task(build_payload, (), {}, None)

    assert isinstance(recorded, PydanticPayload)
    assert recorded == payload
    assert isinstance(appender.events[-1].msg, TaskCompleted)
    assert appender.events[-1].msg.output == {"result": payload.model_dump()}

    replay_appender = FakeAppender()
    replayed = await make_context(appender.events, appender=replay_appender).run_task(build_payload, (), {}, None)

    assert isinstance(replayed, PydanticPayload)
    assert replayed == payload
    assert calls == 1
    assert replay_appender.events == []


async def test_msgspec_struct_task_result_is_typed_from_its_recorded_outcome() -> None:
    payload = StructPayload(name="struct", count=11, tags=["x", "y"])
    calls = 0

    async def build_payload() -> StructPayload:
        nonlocal calls
        calls += 1
        return payload

    appender = FakeAppender()
    recorded = await make_context(appender=appender).run_task(build_payload, (), {}, None)

    assert isinstance(recorded, StructPayload)
    assert recorded == payload
    assert isinstance(appender.events[-1].msg, TaskCompleted)
    assert appender.events[-1].msg.output == {"result": msgspec.to_builtins(payload)}

    replay_appender = FakeAppender()
    replayed = await make_context(appender.events, appender=replay_appender).run_task(build_payload, (), {}, None)

    assert isinstance(replayed, StructPayload)
    assert replayed == payload
    assert calls == 1
    assert replay_appender.events == []


async def test_nested_closure_scoped_forward_msgspec_result_is_typed_when_recorded_and_replayed(in_step) -> None:
    class LocalStruct(msgspec.Struct):
        name: str
        count: int

    calls = 0

    @task
    async def build_payload() -> list[LocalStruct]:
        nonlocal calls
        calls += 1
        return [LocalStruct(name="local", count=13)]

    appender = FakeAppender()
    in_step(make_context(appender=appender))

    recorded = await build_payload()

    assert recorded == [LocalStruct(name="local", count=13)]
    assert all(isinstance(item, LocalStruct) for item in recorded)

    replay_appender = FakeAppender()
    in_step(make_context(appender.events, appender=replay_appender))

    replayed = await build_payload()

    assert replayed == [LocalStruct(name="local", count=13)]
    assert all(isinstance(item, LocalStruct) for item in replayed)
    assert calls == 1
    assert replay_appender.events == []
