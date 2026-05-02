import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

from grctl.models import Directive, HistoryEvent, HistoryKind
from grctl.models.common import ErrorDetails
from grctl.models.directive import RetryPolicy
from grctl.models.history import TaskAttemptFailed, TaskCancelled, TaskCompleted, TaskFailed, TimestampRecorded
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.runtime import NonDeterminismError, StepRuntime, set_step_runtime
from grctl.worker.task import task
from grctl.workflow import Workflow


class UserModel(BaseModel):
    id: str
    name: str


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


def _make_runtime(step_history: list[HistoryEvent] | None = None) -> StepRuntime:
    runtime = StepRuntime(
        workflow=Mock(spec=Workflow),
        worker_id="test-worker",
        directive=Mock(spec=Directive),
        connection=AsyncMock(spec=Connection),
        step_history=step_history if step_history is not None else [],
    )
    runtime.publisher.publish_history = AsyncMock()  # ty:ignore[invalid-assignment]
    runtime.step_name = "step"
    return runtime


def _make_ctx() -> Context:
    return Context(
        run_info=Mock(),
        store=Mock(),
        worker_id=Mock(),
        directive=Mock(),
    )


# Module-level task functions used across tests
@task
async def _task_fetch_user(user_id: int) -> dict:
    return {"id": user_id, "name": "Live"}


@task
async def _task_send_email(email: str) -> bool:
    return True


@task(retry_policy=RetryPolicy(max_attempts=3))
async def _task_always_fails() -> None:
    raise ValueError("always fails")


class TestTaskReplay:
    # Test 16: @task replays from identity match instead of hash scan
    async def test_task_replays_from_cursor(self) -> None:
        # Compute the operation_id that _execute_task will generate (seq=1, first call)
        temp = _make_runtime([])
        op_id = temp.generate_operation_id("_task_fetch_user", {"user_id": 42})

        cached_output = {"id": 42, "name": "Cached"}
        history = [
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id,
                    task_name="_task_fetch_user",
                    step_name="step",
                    output={"result": cached_output},
                    duration_ms=5,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        result = await _task_fetch_user(42)

        assert result == cached_output
        # task_started must NOT be published during replay
        for call in runtime.publisher.publish_history.call_args_list:  # ty:ignore[unresolved-attribute]
            event = call.kwargs["event"]
            assert event.kind != HistoryKind.task_started

    # Test 17: @task records via runtime.record() on first execution
    async def test_task_records_on_live_execution(self) -> None:
        runtime = _make_runtime([])
        set_step_runtime(runtime)

        result = await _task_fetch_user(7)

        assert result == {"id": 7, "name": "Live"}

        # Two publishes: task_started (from TaskExecutor) + task_completed (from runtime.record())
        assert runtime.publisher.publish_history.call_count == 2  # ty:ignore[unresolved-attribute]

        last_event = runtime.publisher.publish_history.call_args.kwargs["event"]  # ty:ignore[unresolved-attribute]
        assert last_event.kind == HistoryKind.task_completed
        assert last_event.operation_id != ""
        assert last_event.msg.output == {"result": {"id": 7, "name": "Live"}}

    # Test 18: Two gathered tasks with different identities resolve in history order
    async def test_gathered_tasks_resolve_in_history_order(self) -> None:
        # history: send_email completed before fetch_user (send_email is at cursor 0)
        temp = _make_runtime([])
        # gather starts fetch_user first (seq=1), then send_email (seq=2)
        op_id_fetch = temp.generate_operation_id("_task_fetch_user", {"user_id": 1})
        op_id_send = temp.generate_operation_id("_task_send_email", {"email": "a@b.com"})

        history = [
            # send_email completed first in original run
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id_send,
                    task_name="_task_send_email",
                    step_name="step",
                    output={"result": True},
                    duration_ms=20,
                ),
                op_id_send,
            ),
            # fetch_user completed second
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id_fetch,
                    task_name="_task_fetch_user",
                    step_name="step",
                    output={"result": {"id": 1, "name": "Cached"}},
                    duration_ms=40,
                ),
                op_id_fetch,
            ),
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        fetch_result, send_result = await asyncio.gather(
            _task_fetch_user(1),
            _task_send_email("a@b.com"),
        )

        assert fetch_result == {"id": 1, "name": "Cached"}
        assert send_result is True

    # Test 20: Mixed sequential + parallel operations replay correctly
    async def test_mixed_sequential_and_task_replay(self) -> None:
        # Step handler calls ctx.now() then a task — sequential, both in history
        temp = _make_runtime([])
        op_id_now = temp.generate_operation_id("now", {})
        op_id_task = temp.generate_operation_id("_task_fetch_user", {"user_id": 99})

        cached_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        cached_output = {"id": 99, "name": "Cached"}

        history = [
            _make_event(HistoryKind.timestamp_recorded, TimestampRecorded(value=cached_ts), op_id_now),
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id_task,
                    task_name="_task_fetch_user",
                    step_name="step",
                    output={"result": cached_output},
                    duration_ms=10,
                ),
                op_id_task,
            ),
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)
        ctx = _make_ctx()

        ts = await ctx.now()
        result = await _task_fetch_user(99)

        assert ts == cached_ts
        assert result == cached_output
        # Nothing published during replay
        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]

    # NonDeterminismError when operation_id not found after yield
    async def test_task_raises_nondeterminism_error_on_missing_operation(self) -> None:
        # History has a task_completed for a DIFFERENT operation — current task will never match
        other_history = [
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id="other:deadbeef",
                    task_name="other",
                    step_name="step",
                    output={"result": None},
                    duration_ms=0,
                ),
                "other:deadbeef",
            )
        ]
        runtime = _make_runtime(other_history)
        set_step_runtime(runtime)

        with pytest.raises(NonDeterminismError):
            await _task_fetch_user(1)


class TestTaskReplayCorrectness:
    # Test 2: TaskFailed in history replays as the original exception
    async def test_failed_task_replays_as_exception(self) -> None:
        temp = _make_runtime([])
        op_id = temp.generate_operation_id("_task_fetch_user", {"user_id": 1})

        history = [
            _make_event(
                HistoryKind.task_failed,
                TaskFailed(
                    task_id=op_id,
                    task_name="_task_fetch_user",
                    step_name="step",
                    error=ErrorDetails(
                        type="ValueError",
                        message="something went wrong",
                        stack_trace="",
                        qualified_type="builtins.ValueError",
                    ),
                    duration_ms=10,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        with pytest.raises(ValueError, match="something went wrong"):
            await _task_fetch_user(1)

        # Nothing published — pure replay
        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]

    # Test 3: TaskCancelled in history re-executes the task live (infrastructure interruption)
    async def test_cancelled_task_re_executes_live(self) -> None:
        temp = _make_runtime([])
        op_id = temp.generate_operation_id("_task_fetch_user", {"user_id": 5})

        history = [
            _make_event(
                HistoryKind.task_cancelled,
                TaskCancelled(
                    task_id=op_id,
                    task_name="_task_fetch_user",
                    step_name="step",
                    duration_ms=2,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        result = await _task_fetch_user(5)

        assert result == {"id": 5, "name": "Live"}
        # task_started + task_completed published for the live re-execution
        kinds = [c.kwargs["event"].kind for c in runtime.publisher.publish_history.call_args_list]  # ty:ignore[unresolved-attribute]
        assert HistoryKind.task_started in kinds
        assert HistoryKind.task_completed in kinds

    # Test 4: Unimportable exception class falls back to RuntimeError
    async def test_failed_task_with_unimportable_exception_falls_back_to_runtime_error(self) -> None:
        temp = _make_runtime([])
        op_id = temp.generate_operation_id("_task_fetch_user", {"user_id": 2})

        history = [
            _make_event(
                HistoryKind.task_failed,
                TaskFailed(
                    task_id=op_id,
                    task_name="_task_fetch_user",
                    step_name="step",
                    error=ErrorDetails(
                        type="GoneError",
                        message="class no longer exists",
                        stack_trace="",
                        qualified_type="myapp.removed_module.GoneError",
                    ),
                    duration_ms=5,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        with pytest.raises(RuntimeError, match="class no longer exists"):
            await _task_fetch_user(2)

    # Test 5: TaskAttemptFailed only in history → next() returns None, task re-executes live
    async def test_attempt_failed_only_in_history_triggers_live_execution(self) -> None:
        temp = _make_runtime([])
        op_id = temp.generate_operation_id("_task_fetch_user", {"user_id": 10})

        history = [
            _make_event(
                HistoryKind.task_attempt_failed,
                TaskAttemptFailed(
                    task_id=op_id,
                    task_name="_task_fetch_user",
                    step_name="step",
                    attempt=1,
                    max_attempts=3,
                    error=ErrorDetails(type="ValueError", message="transient", stack_trace=""),
                    next_retry_delay_ms=100,
                    duration_ms=5,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        # Live execution — task succeeds this time
        result = await _task_fetch_user(10)

        assert result == {"id": 10, "name": "Live"}
        # task_started + task_completed published for the live run
        kinds = [c.kwargs["event"].kind for c in runtime.publisher.publish_history.call_args_list]  # ty:ignore[unresolved-attribute]
        assert HistoryKind.task_started in kinds

    # Test 6: 2x TaskAttemptFailed, max_attempts=3 → starts at attempt 3, no retries on failure
    async def test_retry_count_recovered_no_retries_remaining(self) -> None:
        temp = _make_runtime([])
        op_id = temp.generate_operation_id("_task_always_fails", {})

        attempt_failed_event = TaskAttemptFailed(
            task_id=op_id,
            task_name="_task_always_fails",
            step_name="step",
            attempt=1,
            max_attempts=3,
            error=ErrorDetails(type="ValueError", message="always fails", stack_trace=""),
            next_retry_delay_ms=100,
            duration_ms=5,
        )
        history = [
            _make_event(HistoryKind.task_attempt_failed, attempt_failed_event, op_id),
            _make_event(HistoryKind.task_attempt_failed, attempt_failed_event, op_id),
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        with pytest.raises(ValueError, match="always fails"):
            await _task_always_fails()

        # Only task_started + task_failed — no new task_attempt_failed (no retries remaining)
        kinds = [c.kwargs["event"].kind for c in runtime.publisher.publish_history.call_args_list]  # ty:ignore[unresolved-attribute]
        assert HistoryKind.task_attempt_failed not in kinds
        assert HistoryKind.task_failed in kinds

    # Test 7: 1x TaskAttemptFailed, max_attempts=3 → starts at attempt 2, one retry remaining
    async def test_retry_count_recovered_one_retry_remaining(self) -> None:
        temp = _make_runtime([])
        op_id = temp.generate_operation_id("_task_always_fails", {})

        history = [
            _make_event(
                HistoryKind.task_attempt_failed,
                TaskAttemptFailed(
                    task_id=op_id,
                    task_name="_task_always_fails",
                    step_name="step",
                    attempt=1,
                    max_attempts=3,
                    error=ErrorDetails(type="ValueError", message="always fails", stack_trace=""),
                    next_retry_delay_ms=100,
                    duration_ms=5,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        with pytest.raises(ValueError, match="always fails"):
            await _task_always_fails()

        # One new task_attempt_failed published (attempt 3 retried once before exhausting)
        kinds = [c.kwargs["event"].kind for c in runtime.publisher.publish_history.call_args_list]  # ty:ignore[unresolved-attribute]
        assert kinds.count(HistoryKind.task_attempt_failed) == 1
        assert HistoryKind.task_failed in kinds

    # Test 8: Mix of completed and failed tasks in the same step replay correctly
    async def test_mixed_completed_and_failed_tasks_replay(self) -> None:
        temp = _make_runtime([])
        op_id_fetch = temp.generate_operation_id("_task_fetch_user", {"user_id": 7})
        op_id_email = temp.generate_operation_id("_task_send_email", {"email": "x@y.com"})

        history = [
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id_fetch,
                    task_name="_task_fetch_user",
                    step_name="step",
                    output={"result": {"id": 7, "name": "Cached"}},
                    duration_ms=5,
                ),
                op_id_fetch,
            ),
            _make_event(
                HistoryKind.task_failed,
                TaskFailed(
                    task_id=op_id_email,
                    task_name="_task_send_email",
                    step_name="step",
                    error=ErrorDetails(
                        type="ConnectionError",
                        message="smtp down",
                        stack_trace="",
                        qualified_type="builtins.ConnectionError",
                    ),
                    duration_ms=8,
                ),
                op_id_email,
            ),
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        result = await _task_fetch_user(7)
        assert result == {"id": 7, "name": "Cached"}

        with pytest.raises(ConnectionError, match="smtp down"):
            await _task_send_email("x@y.com")

        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]

    # Test 9: ErrorDetails.qualified_type is captured during live task execution
    async def test_error_details_stores_qualified_type(self) -> None:
        runtime = _make_runtime([])
        set_step_runtime(runtime)

        with pytest.raises(ValueError, match="always fails"):
            await _task_always_fails()

        # Find the TaskFailed event
        task_failed_event = next(
            c.kwargs["event"]
            for c in runtime.publisher.publish_history.call_args_list  # ty:ignore[unresolved-attribute]
            if c.kwargs["event"].kind == HistoryKind.task_failed
        )
        assert task_failed_event.msg.error.qualified_type == "builtins.ValueError"

    # Test 10: Parallel tasks with one failure replay in original history order
    async def test_parallel_tasks_with_failure_replay_in_history_order(self) -> None:
        temp = _make_runtime([])
        # gather spawns fetch_user (seq=1) then send_email (seq=2)
        op_id_fetch = temp.generate_operation_id("_task_fetch_user", {"user_id": 3})
        op_id_email = temp.generate_operation_id("_task_send_email", {"email": "z@z.com"})

        # history: send_email failed first, fetch_user completed second
        history = [
            _make_event(
                HistoryKind.task_failed,
                TaskFailed(
                    task_id=op_id_email,
                    task_name="_task_send_email",
                    step_name="step",
                    error=ErrorDetails(
                        type="OSError",
                        message="network error",
                        stack_trace="",
                        qualified_type="builtins.OSError",
                    ),
                    duration_ms=12,
                ),
                op_id_email,
            ),
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id_fetch,
                    task_name="_task_fetch_user",
                    step_name="step",
                    output={"result": {"id": 3, "name": "Cached"}},
                    duration_ms=20,
                ),
                op_id_fetch,
            ),
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        results = await asyncio.gather(
            _task_fetch_user(3),
            _task_send_email("z@z.com"),
            return_exceptions=True,
        )

        assert results[0] == {"id": 3, "name": "Cached"}
        assert isinstance(results[1], OSError)
        assert str(results[1]) == "network error"
        runtime.publisher.publish_history.assert_not_called()  # ty:ignore[unresolved-attribute]


class TestTaskOutputTypedReplay:
    async def test_task_output_replay_returns_typed_value(self) -> None:
        @task
        async def get_user(user_id: str) -> UserModel:
            return UserModel(id=user_id, name="Live")

        temp = _make_runtime([])
        op_id = temp.generate_operation_id("get_user", {"user_id": "x"})
        history = [
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id,
                    task_name="get_user",
                    step_name="step",
                    output={"result": {"id": "x", "name": "Alice"}},
                    duration_ms=5,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        result = await get_user("x")

        assert isinstance(result, UserModel)
        assert result.id == "x"
        assert result.name == "Alice"

    async def test_task_output_replay_tuple_return(self) -> None:
        @task
        async def get_pair(x: str) -> tuple[str, int]:
            return (x, 0)

        temp = _make_runtime([])
        op_id = temp.generate_operation_id("get_pair", {"x": "hello"})
        history = [
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id,
                    task_name="get_pair",
                    step_name="step",
                    output={"result": ["hello", 42]},
                    duration_ms=5,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        result = await get_pair("hello")

        assert result == ("hello", 42)
        assert isinstance(result, tuple)

    async def test_task_output_replay_none_return(self) -> None:
        @task
        async def do_nothing() -> None:
            pass

        temp = _make_runtime([])
        op_id = temp.generate_operation_id("do_nothing", {})
        history = [
            _make_event(
                HistoryKind.task_completed,
                TaskCompleted(
                    task_id=op_id,
                    task_name="do_nothing",
                    step_name="step",
                    output={"result": None},
                    duration_ms=5,
                ),
                op_id,
            )
        ]
        runtime = _make_runtime(history)
        set_step_runtime(runtime)

        result = await do_nothing()

        assert result is None
