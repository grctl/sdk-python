import asyncio

import pytest

from grctl.models.directive import RetryPolicy
from grctl.worker.task import (
    AttemptFailed,
    Cancelled,
    RetryRunner,
    _calculate_backoff_delay,
)


async def test_success_on_first_attempt():
    async def fn() -> str:
        return "ok"

    runner = RetryRunner(fn, RetryPolicy(max_attempts=3))
    events = [event async for event in runner.execute((), {})]

    assert events == []
    assert runner.result == "ok"


async def test_fail_once_then_succeed():
    call_count = 0

    async def fn() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient")
        return "ok"

    runner = RetryRunner(fn, RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    events = [event async for event in runner.execute((), {})]

    assert len(events) == 1
    assert isinstance(events[0], AttemptFailed)
    assert events[0].attempt == 1
    assert isinstance(events[0].error, RuntimeError)
    assert runner.result == "ok"


async def test_exhaust_retries():
    async def fn() -> None:
        raise RuntimeError("always fails")

    runner = RetryRunner(fn, RetryPolicy(max_attempts=3, initial_delay_ms=1, backoff_multiplier=1.0))
    events: list[AttemptFailed | Cancelled] = []
    with pytest.raises(RuntimeError, match="always fails"):  # noqa: PT012
        async for event in runner.execute((), {}):
            events.append(event)  # noqa: PERF401

    # 3 attempts → 2 AttemptFailed events (last attempt raises)
    assert len(events) == 2
    assert all(isinstance(e, AttemptFailed) for e in events)
    assert isinstance(events[0], AttemptFailed)
    assert events[0].attempt == 1
    assert isinstance(events[1], AttemptFailed)
    assert events[1].attempt == 2


async def test_non_retryable_error_raises_immediately():
    async def fn() -> None:
        raise ValueError("not retryable")

    runner = RetryRunner(
        fn,
        RetryPolicy(max_attempts=3, non_retryable_errors=["ValueError"]),
    )
    events = []
    with pytest.raises(ValueError, match="not retryable"):
        events = [event async for event in runner.execute((), {})]

    assert events == []


async def test_no_policy_raises_immediately():
    async def fn() -> None:
        raise RuntimeError("fail")

    runner = RetryRunner(fn, None)
    events = []
    with pytest.raises(RuntimeError, match="fail"):
        events = [event async for event in runner.execute((), {})]

    assert events == []


async def test_cancelled_error_yields_cancelled_and_reraises():
    async def fn() -> None:
        raise asyncio.CancelledError

    runner = RetryRunner(fn, RetryPolicy(max_attempts=3))
    events: list[AttemptFailed | Cancelled] = []
    with pytest.raises(asyncio.CancelledError):  # noqa: PT012
        async for event in runner.execute((), {}):
            events.append(event)  # noqa: PERF401

    assert len(events) == 1
    assert isinstance(events[0], Cancelled)


async def test_attempt_failed_delay_ms_matches_backoff():
    call_count = 0

    async def fn() -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient")
        return "ok"

    policy = RetryPolicy(max_attempts=3, initial_delay_ms=200, backoff_multiplier=2.0)
    runner = RetryRunner(fn, policy)
    events = [event async for event in runner.execute((), {})]

    assert len(events) == 1
    assert isinstance(events[0], AttemptFailed)
    expected_delay = _calculate_backoff_delay(policy, 1)
    assert events[0].delay_ms == expected_delay


async def test_last_attempt_duration_ms_set_on_terminal_failure():
    async def fn() -> None:
        raise RuntimeError("fail")

    runner = RetryRunner(fn, None)
    with pytest.raises(RuntimeError):
        async for _ in runner.execute((), {}):
            pass

    assert hasattr(runner, "last_attempt_duration_ms")
    assert isinstance(runner.last_attempt_duration_ms, int)
    assert runner.last_attempt_duration_ms >= 0
