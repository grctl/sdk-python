import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from grctl.models import (
    ChildWorkflowStarted,
    Directive,
    HistoryEvent,
    HistoryKind,
    ParentEventSent,
    RandomRecorded,
    RunInfo,
    SleepRecorded,
    TimestampRecorded,
    UuidRecorded,
)
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.runtime import NonDeterminismError, StepRuntime, set_step_runtime
from grctl.workflow import Workflow


def _make_event(kind: HistoryKind, msg, operation_id: str) -> HistoryEvent:
    return HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="w-1",
        timestamp=datetime.now(UTC),
        kind=kind,
        msg=msg,
        operation_id=operation_id,
    )


def _setup_runtime(step_history: list[HistoryEvent]) -> StepRuntime:
    runtime = StepRuntime(
        workflow=Mock(spec=Workflow),
        worker_id="test-worker",
        directive=Mock(spec=Directive),
        connection=AsyncMock(spec=Connection),
        step_history=step_history,
    )
    runtime.publisher.publish_history = AsyncMock()  # ty:ignore[invalid-assignment]
    set_step_runtime(runtime)
    return runtime


def _make_ctx(parent_run: RunInfo | None = None) -> Context:
    return Context(
        run_info=Mock(spec=RunInfo),
        store=Mock(),
        worker_id=Mock(),
        directive=Mock(),
        parent_run=parent_run,
    )


class TestNow:
    # Test 10: ctx.now() returns cached timestamp during replay
    async def test_now_returns_cached_value_during_replay(self) -> None:
        cached = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        runtime = _setup_runtime([])
        operation_id = runtime.generate_operation_id("now", {})
        history = [_make_event(HistoryKind.timestamp_recorded, TimestampRecorded(value=cached), operation_id)]
        runtime = _setup_runtime(history)
        ctx = _make_ctx()

        result = await ctx.now()

        assert result == cached

    # Test 11: ctx.now() generates and records timestamp during live execution
    async def test_now_records_during_live_execution(self) -> None:
        runtime = _setup_runtime([])
        ctx = _make_ctx()

        result = await ctx.now()

        assert isinstance(result, datetime)
        assert result.tzinfo is UTC
        runtime.publisher.publish_history.assert_called_once()  # ty:ignore[unresolved-attribute]


class TestRandom:
    # Test 12: ctx.random() replays cached value
    async def test_random_returns_cached_value_during_replay(self) -> None:
        runtime = _setup_runtime([])
        operation_id = runtime.generate_operation_id("random", {})
        history = [_make_event(HistoryKind.random_recorded, RandomRecorded(value=0.42), operation_id)]
        runtime = _setup_runtime(history)
        ctx = _make_ctx()

        result = await ctx.random()

        assert result == 0.42

    async def test_random_records_during_live_execution(self) -> None:
        runtime = _setup_runtime([])
        ctx = _make_ctx()

        result = await ctx.random()

        assert isinstance(result, float)
        runtime.publisher.publish_history.assert_called_once()  # ty:ignore[unresolved-attribute]


class TestUuid4:
    # Test 13: ctx.uuid4() replays cached value
    async def test_uuid4_returns_cached_value_during_replay(self) -> None:
        cached = uuid.uuid4()
        runtime = _setup_runtime([])
        operation_id = runtime.generate_operation_id("uuid4", {})
        history = [_make_event(HistoryKind.uuid_recorded, UuidRecorded(value=str(cached)), operation_id)]
        runtime = _setup_runtime(history)
        ctx = _make_ctx()

        result = await ctx.uuid4()

        assert result == cached

    async def test_uuid4_records_during_live_execution(self) -> None:
        runtime = _setup_runtime([])
        ctx = _make_ctx()

        result = await ctx.uuid4()

        assert isinstance(result, uuid.UUID)
        runtime.publisher.publish_history.assert_called_once()  # ty:ignore[unresolved-attribute]


class TestSleep:
    # Test 14: ctx.sleep() resolves instantly during replay — no actual sleep, no recording
    async def test_sleep_skips_actual_sleep_during_replay(self) -> None:
        duration_ms = 5000
        runtime = _setup_runtime([])
        operation_id = runtime.generate_operation_id("sleep", {"duration_ms": duration_ms})
        history = [_make_event(HistoryKind.sleep_recorded, SleepRecorded(duration_ms=duration_ms), operation_id)]
        runtime = _setup_runtime(history)
        ctx = _make_ctx()

        with patch("grctl.worker.context.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await ctx.sleep(timedelta(seconds=5))
            # Only the internal yield sleep(0) may fire; the actual 5s sleep must not
            assert not any(c.args[0] == 5.0 for c in mock_sleep.call_args_list)

        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]

    # Test 15: ctx.sleep() records duration and actually sleeps during live execution
    async def test_sleep_records_and_sleeps_during_live_execution(self) -> None:
        runtime = _setup_runtime([])
        ctx = _make_ctx()

        with patch("grctl.worker.context.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await ctx.sleep(timedelta(seconds=3))
            mock_sleep.assert_called_once_with(3.0)

        runtime.publisher.publish_history.assert_called_once()  # ty:ignore[unresolved-attribute]
        event = runtime.publisher.publish_history.call_args.kwargs["event"]  # ty:ignore[unresolved-attribute]
        assert event.kind == HistoryKind.sleep_recorded
        assert event.msg.duration_ms == 3000


class TestStart:
    # Test 1: live path — generates run_id, calls handle.start(), records ChildWorkflowStarted
    async def test_live_execution_generates_run_id_and_records(self) -> None:
        runtime = _setup_runtime([])
        runtime.publisher.publish_cmd = AsyncMock()  # ty:ignore[invalid-assignment]
        ctx = _make_ctx()

        with patch("grctl.worker.context.WorkflowHandle") as mock_handle_cls:
            mock_handle = AsyncMock()
            mock_handle_cls.return_value = mock_handle

            result = await ctx.start("order", "order-1")

        mock_handle.start.assert_called_once()
        runtime.publisher.publish_history.assert_called_once()  # ty:ignore[unresolved-attribute]
        event = runtime.publisher.publish_history.call_args.kwargs["event"]  # ty:ignore[unresolved-attribute]
        assert event.kind == HistoryKind.child_started
        assert event.msg.wf_type == "order"
        assert event.msg.wf_id == "order-1"
        assert result is mock_handle

    # Test 2: replay path — returns handle with original run_id, skips handle.start()
    async def test_replay_returns_original_run_id_and_skips_start(self) -> None:
        original_run_id = "01JAAAAAAAAAAAAAAAAAAAAAAA"
        runtime = _setup_runtime([])
        operation_id = runtime.generate_operation_id(
            "start",
            {"wf_type": "order", "wf_id": "order-1", "workflow_input": None, "workflow_timeout": None},
        )
        history = [
            _make_event(
                HistoryKind.child_started,
                ChildWorkflowStarted(run_id=original_run_id, wf_type="order", wf_id="order-1"),
                operation_id,
            )
        ]
        runtime = _setup_runtime(history)
        ctx = _make_ctx()

        with patch("grctl.worker.context.WorkflowHandle") as mock_handle_cls:
            mock_handle = AsyncMock()
            mock_handle_cls.return_value = mock_handle

            await ctx.start("order", "order-1")

        # handle.start() must NOT be called on replay
        mock_handle.start.assert_not_called()
        # history must NOT be published again
        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]
        # run_id passed to WorkflowHandle constructor must match original
        constructed_run_info = mock_handle_cls.call_args.kwargs["run_info"]
        assert constructed_run_info.id == original_run_id

    # Test 3: kind mismatch at matched operation_id raises NonDeterminismError
    async def test_kind_mismatch_raises_non_determinism_error(self) -> None:
        runtime = _setup_runtime([])
        operation_id = runtime.generate_operation_id(
            "start",
            {"wf_type": "order", "wf_id": "order-1", "workflow_input": None, "workflow_timeout": None},
        )
        # Record a timestamp event under the same operation_id — wrong kind
        history = [
            _make_event(HistoryKind.timestamp_recorded, TimestampRecorded(value=datetime.now(UTC)), operation_id)
        ]
        runtime = _setup_runtime(history)
        ctx = _make_ctx()

        with pytest.raises(NonDeterminismError):
            await ctx.start("order", "order-1")

    # Test 4: two ctx.start() calls with same wf_type + wf_id produce different operation_ids (seq tiebreaker)
    async def test_two_identical_starts_produce_different_operation_ids(self) -> None:
        runtime = _setup_runtime([])
        runtime.publisher.publish_cmd = AsyncMock()  # ty:ignore[invalid-assignment]
        ctx = _make_ctx()

        ids: list[str] = []

        original_generate = runtime.generate_operation_id

        def capturing_generate(fn_name: str, args: dict) -> str:  # type: ignore[type-arg]
            op_id = original_generate(fn_name, args)
            ids.append(op_id)
            return op_id

        runtime.generate_operation_id = capturing_generate  # ty:ignore[invalid-assignment]

        with patch("grctl.worker.context.WorkflowHandle") as mock_handle_cls:
            mock_handle_cls.return_value = AsyncMock()
            await ctx.start("order", "order-1")
            await ctx.start("order", "order-1")

        assert len(ids) == 2
        assert ids[0] != ids[1]


class TestSendToParent:
    # Test 5: live path — publishes command and records ParentEventSent
    async def test_live_execution_publishes_and_records(self) -> None:
        runtime = _setup_runtime([])
        runtime.publisher.publish_cmd = AsyncMock()  # ty:ignore[invalid-assignment]
        parent_run = Mock(spec=RunInfo)
        parent_run.wf_id = "parent-wf"
        ctx = _make_ctx(parent_run=parent_run)

        await ctx.send_to_parent("order.completed", {"order_id": "42"})

        runtime.publisher.publish_cmd.assert_called_once()  # ty:ignore[unresolved-attribute]
        runtime.publisher.publish_history.assert_called_once()  # ty:ignore[unresolved-attribute]
        event = runtime.publisher.publish_history.call_args.kwargs["event"]  # ty:ignore[unresolved-attribute]
        assert event.kind == HistoryKind.parent_event_sent
        assert event.msg.event_name == "order.completed"

    # Test 6: replay path — skips publish
    async def test_replay_skips_publish(self) -> None:
        runtime = _setup_runtime([])
        parent_run = Mock(spec=RunInfo)
        parent_run.wf_id = "parent-wf"
        parent_run.wf_type = "parent-wf"
        operation_id = runtime.generate_operation_id(
            "send_to_parent", {"event_name": "order.completed", "payload": None}
        )
        history = [
            _make_event(
                HistoryKind.parent_event_sent,
                ParentEventSent(
                    event_name="order.completed",
                    payload=None,
                    parent_wf_type=parent_run.wf_type,
                    parent_wf_id=parent_run.wf_id,
                ),
                operation_id,
            )
        ]
        runtime = _setup_runtime(history)
        runtime.publisher.publish_cmd = AsyncMock()  # ty:ignore[invalid-assignment]

        ctx = _make_ctx(parent_run=parent_run)

        await ctx.send_to_parent("order.completed")

        runtime.publisher.publish_cmd.assert_not_called()  # ty:ignore[unresolved-attribute]
        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]

    # Test 7: kind mismatch raises NonDeterminismError
    async def test_kind_mismatch_raises_non_determinism_error(self) -> None:
        runtime = _setup_runtime([])
        operation_id = runtime.generate_operation_id(
            "send_to_parent", {"event_name": "order.completed", "payload": None}
        )
        history = [
            _make_event(HistoryKind.timestamp_recorded, TimestampRecorded(value=datetime.now(UTC)), operation_id)
        ]
        runtime = _setup_runtime(history)
        runtime.publisher.publish_cmd = AsyncMock()  # ty:ignore[invalid-assignment]
        parent_run = Mock(spec=RunInfo)
        parent_run.wf_id = "parent-wf"
        ctx = _make_ctx(parent_run=parent_run)

        with pytest.raises(NonDeterminismError):
            await ctx.send_to_parent("order.completed")

    # Test 8: no parent run raises RuntimeError before any replay logic
    async def test_no_parent_run_raises_runtime_error(self) -> None:
        _setup_runtime([])
        ctx = _make_ctx(parent_run=None)

        with pytest.raises(RuntimeError, match="No parent workflow"):
            await ctx.send_to_parent("order.completed")


class TestSequentialReplay:
    # Test 9: sequential ctx.start() then ctx.send_to_parent() replay correctly in order
    async def test_sequential_start_then_send_to_parent_replay(self) -> None:
        original_run_id = "01JAAAAAAAAAAAAAAAAAAAAAAB"
        runtime = _setup_runtime([])
        parent_run = Mock(spec=RunInfo)
        parent_run.wf_id = "parent-wf"
        parent_run.wf_type = "parent-wf"

        op1 = runtime.generate_operation_id(
            "start",
            {"wf_type": "order", "wf_id": "order-1", "workflow_input": None, "workflow_timeout": None},
        )
        op2 = runtime.generate_operation_id("send_to_parent", {"event_name": "started", "payload": None})

        history = [
            _make_event(
                HistoryKind.child_started,
                ChildWorkflowStarted(run_id=original_run_id, wf_type="order", wf_id="order-1"),
                op1,
            ),
            _make_event(
                HistoryKind.parent_event_sent,
                ParentEventSent(
                    event_name="started", payload=None, parent_wf_type=parent_run.wf_type, parent_wf_id=parent_run.wf_id
                ),
                op2,
            ),
        ]
        runtime = _setup_runtime(history)
        runtime.publisher.publish_cmd = AsyncMock()  # ty:ignore[invalid-assignment]

        ctx = _make_ctx(parent_run=parent_run)

        with patch("grctl.worker.context.WorkflowHandle") as mock_handle_cls:
            mock_handle = AsyncMock()
            mock_handle_cls.return_value = mock_handle
            await ctx.start("order", "order-1")
            await ctx.send_to_parent("started")

        mock_handle.start.assert_not_called()
        runtime.publisher.publish_cmd.assert_not_called()  # ty:ignore[unresolved-attribute]
        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]

    # Test 10: ctx.start() after ctx.now() resolves at correct cursor position
    async def test_start_after_now_resolves_at_correct_cursor(self) -> None:
        original_run_id = "01JAAAAAAAAAAAAAAAAAAAAAAC"
        cached_ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
        runtime = _setup_runtime([])

        op_now = runtime.generate_operation_id("now", {})
        op_start = runtime.generate_operation_id(
            "start",
            {"wf_type": "ship", "wf_id": "ship-1", "workflow_input": None, "workflow_timeout": None},
        )

        history = [
            _make_event(HistoryKind.timestamp_recorded, TimestampRecorded(value=cached_ts), op_now),
            _make_event(
                HistoryKind.child_started,
                ChildWorkflowStarted(run_id=original_run_id, wf_type="ship", wf_id="ship-1"),
                op_start,
            ),
        ]
        runtime = _setup_runtime(history)
        ctx = _make_ctx()

        ts = await ctx.now()
        assert ts == cached_ts

        with patch("grctl.worker.context.WorkflowHandle") as mock_handle_cls:
            mock_handle = AsyncMock()
            mock_handle_cls.return_value = mock_handle
            await ctx.start("ship", "ship-1")

        mock_handle.start.assert_not_called()
        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]
        constructed_run_info = mock_handle_cls.call_args.kwargs["run_info"]
        assert constructed_run_info.id == original_run_id
