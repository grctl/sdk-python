import asyncio
import uuid
from datetime import timedelta

from grctl.models import HistoryKind
from tests.unit.exec.fakes import FakeAppender, make_context, record_then_replay


async def test_now_records_and_replays_the_same_value() -> None:
    result = await record_then_replay(make_context, lambda ctx: ctx.now())

    assert len(result.events) == 1
    assert result.events[0].kind == HistoryKind.timestamp_recorded


async def test_random_records_and_replays_the_same_value() -> None:
    result = await record_then_replay(make_context, lambda ctx: ctx.random())

    assert result.events[0].kind == HistoryKind.random_recorded


async def test_uuid4_records_and_replays_the_same_value() -> None:
    result = await record_then_replay(make_context, lambda ctx: ctx.uuid4())

    assert isinstance(result.value, uuid.UUID)
    assert result.events[0].kind == HistoryKind.uuid_recorded


async def test_sleep_records_and_skips_the_wait_on_replay() -> None:
    # record_then_replay isn't used here: unlike the other ops, sleep's whole point is that
    # record and replay take different amounts of real time, which the harness can't express.
    appender = FakeAppender()
    ctx = make_context(appender=appender)

    # Long enough that the replay assertion below would time out if the wait weren't skipped.
    duration = timedelta(seconds=2)
    await ctx.sleep(duration)

    assert len(appender.events) == 1
    assert appender.events[0].kind == HistoryKind.sleep_recorded

    history = appender.events  # becomes the replay run's step history
    replay_appender = FakeAppender()
    replay_ctx = make_context(history, appender=replay_appender)

    # Same duration as the recorded call — only passes quickly if replay skips the actual wait.
    await asyncio.wait_for(replay_ctx.sleep(duration), timeout=1)

    assert replay_appender.events == []
