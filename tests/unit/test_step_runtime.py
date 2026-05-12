import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from grctl.models import Directive, HistoryEvent, HistoryKind, RunStarted, TaskCompleted, TaskFailed
from grctl.models.common import ErrorDetails
from grctl.models.history import HistoryEvents
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.runtime import NonDeterminismError, StepRuntime, _generate_operation_id
from grctl.workflow import Workflow


def _make_runtime(step_history: list[HistoryEvent] | None = None) -> StepRuntime:
    return StepRuntime(
        workflow=Mock(spec=Workflow),
        worker_id="test-worker",
        directive=Mock(spec=Directive),
        connection=AsyncMock(spec=Connection),
        step_history=step_history if step_history is not None else [],
    )


def _make_event(kind: HistoryKind, msg: HistoryEvents, operation_id: str) -> HistoryEvent:
    return HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="w-1",
        timestamp=datetime.now(UTC),
        kind=kind,
        msg=msg,
        operation_id=operation_id,
    )


class TestStepRuntime:
    async def test_record(self):
        runtime = _make_runtime()
        publish_history = AsyncMock()
        runtime.publisher.publish_history = publish_history  # ty:ignore[invalid-assignment]
        await runtime.record(HistoryKind.run_started, RunStarted(), operation_id="")

        publish_history.assert_called_once()

    # Test 5: record() publishes HistoryEvent with correct operation_id and envelope
    async def test_record_publishes_event_with_operation_id(self):
        runtime = _make_runtime()
        publish_history = AsyncMock()
        runtime.publisher.publish_history = publish_history  # ty:ignore[invalid-assignment]

        msg = TaskCompleted(task_id="t-1", task_name="fetch", output={"result": 42}, step_name="s", duration_ms=10)
        await runtime.record(HistoryKind.task_completed, msg, operation_id="fetch:abc123")

        publish_history.assert_called_once()
        event = publish_history.call_args.kwargs["event"]
        assert event.operation_id == "fetch:abc123"
        assert event.kind == HistoryKind.task_completed
        assert event.msg is msg

    def test_get_step_context(self):
        runtime = _make_runtime()
        runtime.workflow._step_handlers = {}
        context = runtime.get_step_context()
        assert isinstance(context, Context)

    # Test 6: Seq counter increments only on generate_operation_id, not next() or record()
    async def test_seq_increments_on_generate_operation_id(self):
        runtime = _make_runtime()
        runtime.publisher.publish_history = AsyncMock()  # ty:ignore[invalid-assignment]

        assert runtime._seq == 0

        runtime.generate_operation_id("op", {})
        assert runtime._seq == 1

        runtime.generate_operation_id("op", {})
        assert runtime._seq == 2

        await runtime.next(HistoryKind.task_completed, "op-1")
        assert runtime._seq == 2  # next() does not increment

        await runtime.record(HistoryKind.run_started, RunStarted(), operation_id="")
        assert runtime._seq == 2  # record() does not increment

    # Test 7: Same function + same args + different seq produces different operation_id
    def test_same_fn_same_args_different_seq_produces_different_operation_id(self):
        op_id_1 = _generate_operation_id("fetch_user", {"user_id": 42}, seq=0)
        op_id_2 = _generate_operation_id("fetch_user", {"user_id": 42}, seq=1)

        assert op_id_1 != op_id_2
        assert op_id_1.startswith("fetch_user:")
        assert op_id_2.startswith("fetch_user:")

    # Test 19: Two sequential calls with same fn+args get different operation_ids (seq tiebreaker)
    def test_duplicate_calls_get_unique_operation_ids_via_seq(self):
        runtime = _make_runtime()
        op_id_a = runtime.generate_operation_id("notify_user", {"user_id": 42})
        op_id_b = runtime.generate_operation_id("notify_user", {"user_id": 42})

        assert op_id_a != op_id_b


class TestCursorReplay:
    # Test 1: next() returns None when history is empty
    async def test_next_returns_none_when_history_empty(self):
        runtime = _make_runtime(step_history=[])
        result = await runtime.next(HistoryKind.task_completed, "op-1")
        assert result is None

    # Test 2: next() returns future with cached value when operation_id matches
    async def test_next_returns_future_with_cached_value(self):
        msg = TaskCompleted(task_id="t-1", task_name="fetch", output={"result": 42}, step_name="s", duration_ms=10)
        history = [_make_event(HistoryKind.task_completed, msg, "op-1")]
        runtime = _make_runtime(step_history=history)

        future = await runtime.next(HistoryKind.task_completed, "op-1")

        assert future is not None
        assert future.result() is msg

    # Test 3: next() raises NonDeterminismError when a replay kind mismatches expected kind
    async def test_next_raises_on_kind_mismatch(self):
        # history has task_failed but caller expects task_completed — a non-determinism mismatch
        msg = TaskFailed(
            task_id="t-1",
            task_name="fetch",
            step_name="s",
            error=ErrorDetails(type="ValueError", message="oops", stack_trace=""),
            duration_ms=10,
        )
        history = [_make_event(HistoryKind.task_failed, msg, "op-1")]
        runtime = _make_runtime(step_history=history)

        future = await runtime.next(HistoryKind.task_completed, "op-1")

        assert future is not None
        with pytest.raises(NonDeterminismError):
            future.result()

    # Test 4: next() returns None for operation past history (replay-to-live transition)
    async def test_next_returns_none_past_history(self):
        msg = TaskCompleted(task_id="t-1", task_name="fetch", output={"result": 42}, step_name="s", duration_ms=10)
        history = [_make_event(HistoryKind.task_completed, msg, "op-1")]
        runtime = _make_runtime(step_history=history)

        # Consume the history entry
        await runtime.next(HistoryKind.task_completed, "op-1")
        # Now past history — should return None
        result = await runtime.next(HistoryKind.task_completed, "op-2")
        assert result is None

    # Test 8: Parallel futures resolve in history order
    async def test_parallel_futures_resolve_in_history_order(self):
        msg_b = TaskCompleted(task_id="t-b", task_name="B", output={"result": "b-out"}, step_name="s", duration_ms=10)
        msg_a = TaskCompleted(task_id="t-a", task_name="A", output={"result": "a-out"}, step_name="s", duration_ms=20)
        # history order: B completed first, then A
        history = [
            _make_event(HistoryKind.task_completed, msg_b, "op-B"),
            _make_event(HistoryKind.task_completed, msg_a, "op-A"),
        ]
        runtime = _make_runtime(step_history=history)

        async def resolve_a() -> asyncio.Future[HistoryEvents] | None:
            return await runtime.next(HistoryKind.task_completed, "op-A")

        async def resolve_b() -> asyncio.Future[HistoryEvents] | None:
            return await runtime.next(HistoryKind.task_completed, "op-B")

        # A registers first (doesn't match cursor[0]=B), B registers second (matches, chains through)
        future_a, future_b = await asyncio.gather(resolve_a(), resolve_b())

        assert future_a is not None
        assert future_b is not None
        assert future_b.result() is msg_b
        assert future_a.result() is msg_a

    # Test 9: Unresolved future after yield raises NonDeterminismError
    async def test_unresolved_future_raises_non_determinism_error(self):
        msg_x = TaskCompleted(task_id="t-x", task_name="X", output={"result": "x"}, step_name="s", duration_ms=10)
        msg_y = TaskCompleted(task_id="t-y", task_name="Y", output={"result": "y"}, step_name="s", duration_ms=20)
        # history has [X, Y] but only Y registers — X blocks the cursor
        history = [
            _make_event(HistoryKind.task_completed, msg_x, "op-X"),
            _make_event(HistoryKind.task_completed, msg_y, "op-Y"),
        ]
        runtime = _make_runtime(step_history=history)

        with pytest.raises(NonDeterminismError):
            await runtime.next(HistoryKind.task_completed, "op-Y")
