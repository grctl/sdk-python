import pytest

from grctl.models import HistoryKind
from tests.unit.exec.fakes import FakeAppender, make_context, record_then_replay


async def test_context_run_executes_and_records_a_simple_async_function() -> None:
    appender = FakeAppender()
    ctx = make_context(appender=appender)

    async def add(a: int, b: int) -> int:
        return a + b

    result = await ctx.run(add, 1, 2)

    assert result == 3
    assert len(appender.events) == 1
    assert appender.events[0].kind == HistoryKind.task_completed


async def test_context_run_replays_without_recalling_the_function() -> None:
    calls = 0

    async def add(a: int, b: int) -> int:
        nonlocal calls
        calls += 1
        return a + b

    result = await record_then_replay(make_context, lambda ctx: ctx.run(add, 1, 2))

    assert result.value == 3
    assert calls == 1  # not called again on replay
    assert result.events[0].kind == HistoryKind.task_completed


async def test_context_run_reraises_on_failure() -> None:
    appender = FakeAppender()
    ctx = make_context(appender=appender)

    async def boom() -> None:
        raise ValueError("bad input")

    with pytest.raises(RuntimeError, match="bad input"):
        await ctx.run(boom)

    assert appender.events[0].kind == HistoryKind.task_failed
