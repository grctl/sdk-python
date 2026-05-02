import asyncio
import functools
import importlib
import inspect
import random
import traceback
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast, get_type_hints, overload

from grctl.logging_config import get_logger
from grctl.models.directive import RetryPolicy
from grctl.models.history import (
    ErrorDetails,
    HistoryEvent,
    HistoryKind,
    TaskAttemptFailed,
    TaskCancelled,
    TaskCompleted,
    TaskEvents,
    TaskFailed,
    TaskStarted,
)
from grctl.worker.codec import CodecRegistry
from grctl.worker.runtime import get_step_runtime

if TYPE_CHECKING:
    from grctl.nats.publisher import Publisher

logger = get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def _capture_args(fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Capture function arguments as a dict."""
    sig = inspect.signature(fn)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()
    return dict(bound.arguments)


def _normalize_args(args_dict: dict[str, Any], codec: CodecRegistry) -> dict[str, Any]:
    return {k: codec.to_primitive(v) for k, v in args_dict.items()}


def _calculate_backoff_delay(policy: RetryPolicy, attempt: int) -> int:
    """Calculate backoff delay in milliseconds for a given attempt number."""
    initial = policy.initial_delay_ms or 100
    multiplier = policy.backoff_multiplier or 2.0
    max_delay = policy.max_delay_ms or 5000
    jitter = policy.jitter or 0.0

    delay = min(initial * (multiplier ** (attempt - 1)), max_delay)

    if jitter > 0:
        delay += random.uniform(0, jitter) * delay  # noqa: S311

    return int(delay)


def _reconstruct_error(error: ErrorDetails) -> Exception:
    if not error.qualified_type:
        return RuntimeError(error.message)
    try:
        module_name, class_name = error.qualified_type.rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        if not (isinstance(cls, type) and issubclass(cls, Exception)):
            return RuntimeError(error.message)
        return cls(error.message)
    except Exception:
        return RuntimeError(error.message)


def _is_error_retryable(error: Exception, policy: RetryPolicy) -> bool:
    """Check if an error should be retried based on the retry policy filters."""
    error_type = type(error).__name__

    if policy.retryable_errors is None and policy.non_retryable_errors is None:
        return True

    if policy.retryable_errors is not None and policy.non_retryable_errors is None:
        return error_type in policy.retryable_errors

    if policy.retryable_errors is None and policy.non_retryable_errors is not None:
        return error_type not in policy.non_retryable_errors

    # Both set: must be in retryable AND not in non_retryable
    return error_type in policy.retryable_errors and error_type not in policy.non_retryable_errors  # ty:ignore[unsupported-operator]


@dataclass
class AttemptFailed:
    attempt: int
    max_attempts: int
    error: Exception
    stack_trace: str
    delay_ms: int
    duration_ms: int


@dataclass
class Cancelled:
    duration_ms: int


class RetryRunner:
    result: Any
    last_attempt_duration_ms: int

    def __init__(self, fn: Callable[..., Awaitable[Any]], policy: RetryPolicy | None, initial_attempt: int = 0) -> None:
        self._fn = fn
        self._policy = policy
        self._max_attempts = (policy.max_attempts if policy else None) or 1
        self._initial_attempt = initial_attempt

    async def execute(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> AsyncGenerator[AttemptFailed | Cancelled]:
        attempt = self._initial_attempt
        while True:
            attempt += 1
            attempt_start = datetime.now(UTC)
            try:
                result = await self._fn(*args, **kwargs)
            except asyncio.CancelledError:
                duration_ms = int((datetime.now(UTC) - attempt_start).total_seconds() * 1000)
                yield Cancelled(duration_ms=duration_ms)
                raise
            except Exception as e:
                duration_ms = int((datetime.now(UTC) - attempt_start).total_seconds() * 1000)
                stack_trace = traceback.format_exc()
                self.last_attempt_duration_ms = duration_ms
                can_retry = (
                    self._policy is not None and attempt < self._max_attempts and _is_error_retryable(e, self._policy)
                )
                if can_retry and self._policy is not None:
                    delay_ms = _calculate_backoff_delay(self._policy, attempt)
                    yield AttemptFailed(
                        attempt=attempt,
                        max_attempts=self._max_attempts,
                        error=e,
                        stack_trace=stack_trace,
                        delay_ms=delay_ms,
                        duration_ms=duration_ms,
                    )
                    await asyncio.sleep(delay_ms / 1000.0)
                    continue
                raise
            else:
                self.result = result
                return


class TaskExecutor:
    def __init__(  # noqa: PLR0913
        self,
        fn: Callable[..., Awaitable[Any]],
        retry_policy: RetryPolicy | None,
        publisher: "Publisher",
        run_info: Any,
        worker_id: str,
        operation_id: str,
        task_name: str,
        step_name: str,
        codec: CodecRegistry,
        initial_attempt: int = 0,
    ) -> None:
        self._fn = fn
        self._retry_policy = retry_policy
        self._publisher = publisher
        self._run_info = run_info
        self._worker_id = worker_id
        self._operation_id = operation_id
        self._task_name = task_name
        self._step_name = step_name
        self._codec = codec
        self._initial_attempt = initial_attempt
        self._max_attempts = (retry_policy.max_attempts if retry_policy else None) or 1

    def _build_history_event(
        self, kind: HistoryKind, payload: TaskEvents, *, timestamp: datetime | None = None
    ) -> HistoryEvent:
        return HistoryEvent(
            wf_id=self._run_info.wf_id,
            run_id=self._run_info.id,
            worker_id=self._worker_id,
            timestamp=timestamp or datetime.now(UTC),
            kind=kind,
            msg=payload,
            operation_id=self._operation_id,
        )

    async def run(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        start_time = datetime.now(UTC)
        captured = _capture_args(self._fn, args, kwargs)
        normalized = _normalize_args(captured, self._codec)

        await self._publisher.publish_history(
            run_info=self._run_info,
            event=self._build_history_event(
                HistoryKind.task_started,
                TaskStarted(
                    task_id=self._operation_id,
                    task_name=self._task_name,
                    args=normalized,
                    step_name=self._step_name,
                ),
                timestamp=start_time,
            ),
        )

        return await self._run_with_retries(args, kwargs)

    async def _publish_attempt_failed(self, event: AttemptFailed) -> None:
        logger.warning(
            f"Task {self._task_name} attempt {event.attempt}/{self._max_attempts} failed "
            f"in step {self._step_name}, retrying in {event.delay_ms}ms"
        )
        await self._publisher.publish_history(
            run_info=self._run_info,
            event=self._build_history_event(
                HistoryKind.task_attempt_failed,
                TaskAttemptFailed(
                    task_id=self._operation_id,
                    task_name=self._task_name,
                    step_name=self._step_name,
                    attempt=event.attempt,
                    max_attempts=self._max_attempts,
                    error=ErrorDetails(
                        type=type(event.error).__name__,
                        message=str(event.error),
                        stack_trace=event.stack_trace,
                        qualified_type=f"{type(event.error).__module__}.{type(event.error).__qualname__}",
                    ),
                    next_retry_delay_ms=event.delay_ms,
                    duration_ms=event.duration_ms,
                ),
            ),
        )

    async def _publish_cancelled(self, event: Cancelled) -> None:
        logger.warning(f"Task {self._task_name} cancelled in step {self._step_name}")
        await self._publisher.publish_history(
            run_info=self._run_info,
            event=self._build_history_event(
                HistoryKind.task_cancelled,
                TaskCancelled(
                    task_id=self._operation_id,
                    task_name=self._task_name,
                    step_name=self._step_name,
                    duration_ms=event.duration_ms,
                ),
            ),
        )

    async def _publish_failed(self, error: Exception, stack_trace: str, duration_ms: int) -> None:
        logger.exception(f"Task {self._task_name} failed in step {self._step_name}")
        await self._publisher.publish_history(
            run_info=self._run_info,
            event=self._build_history_event(
                HistoryKind.task_failed,
                TaskFailed(
                    task_id=self._operation_id,
                    task_name=self._task_name,
                    step_name=self._step_name,
                    error=ErrorDetails(
                        type=type(error).__name__,
                        message=str(error),
                        stack_trace=stack_trace,
                        qualified_type=f"{type(error).__module__}.{type(error).__qualname__}",
                    ),
                    duration_ms=duration_ms,
                ),
            ),
        )

    async def _run_with_retries(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        runner = RetryRunner(self._fn, self._retry_policy, initial_attempt=self._initial_attempt)
        try:
            async for event in runner.execute(args, kwargs):
                match event:
                    case AttemptFailed():
                        await self._publish_attempt_failed(event)
                    case Cancelled():
                        await self._publish_cancelled(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await self._publish_failed(e, traceback.format_exc(), runner.last_attempt_duration_ms)
            raise
        return runner.result


@overload
def task(fn: Callable[P, Awaitable[R]], /) -> Callable[P, Awaitable[R]]: ...  # noqa: UP047


@overload
def task(*, retry_policy: RetryPolicy) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]: ...


def task(  # noqa: UP047
    fn: Callable[P, Awaitable[R]] | None = None,
    /,
    *,
    retry_policy: RetryPolicy | None = None,
) -> Callable[P, Awaitable[R]] | Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Mark a function as a task and publish lifecycle events.

    Supports both @task and @task(retry_policy=...) forms.
    """

    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await _execute_task(fn, retry_policy, args, kwargs)

        return cast("Callable[P, Awaitable[R]]", wrapper)

    if fn is not None:
        return decorator(fn)

    return decorator


async def _execute_task(
    fn: Callable[..., Awaitable[Any]],
    retry_policy: RetryPolicy | None,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    runtime = get_step_runtime()
    publisher: Publisher = runtime.connection.publisher
    run_info = runtime.run_info
    step_name = runtime.step_name

    # No worker context — run the function directly
    if publisher is None or run_info is None:
        return await fn(*args, **kwargs)

    task_name = fn.__name__  # ty:ignore[unresolved-attribute]
    args_dict = _capture_args(fn, args, kwargs)
    normalized_args = _normalize_args(args_dict, runtime.codec)
    operation_id = runtime.generate_operation_id(task_name, normalized_args)

    # If it's a replaying run, wait for the task outcome from the step history (in-memory).
    # Otherwise, execute the task live.
    future = await runtime.next(
        frozenset({HistoryKind.task_completed, HistoryKind.task_failed, HistoryKind.task_cancelled}),
        operation_id,
    )
    if future is not None:
        logger.info(f"Replaying outcome for task {task_name} ({operation_id}) in step {step_name}")
        event = await future
        if isinstance(event, TaskCompleted):
            raw = event.output["result"]
            try:
                return_type = get_type_hints(fn).get("return")
            except Exception:
                return_type = None
            if return_type is not None:
                return runtime.codec.from_primitive(raw, return_type)
            return raw
        if isinstance(event, TaskFailed):
            raise _reconstruct_error(event.error)
        # TaskCancelled — task didn't finish; fall through to execute it live

    previous_attempts = sum(
        1
        for e in (runtime.step_history or [])
        if e.kind == HistoryKind.task_attempt_failed and e.operation_id == operation_id
    )

    executor = TaskExecutor(
        fn=fn,
        retry_policy=retry_policy,
        publisher=publisher,
        run_info=run_info,
        worker_id=runtime.worker_id,
        operation_id=operation_id,
        task_name=task_name,
        step_name=step_name,
        codec=runtime.codec,
        initial_attempt=previous_attempts,
    )

    task_start = datetime.now(UTC)
    result = await executor.run(args, kwargs)
    duration_ms = int((datetime.now(UTC) - task_start).total_seconds() * 1000)

    await runtime.record(
        HistoryKind.task_completed,
        TaskCompleted(
            task_id=operation_id,
            task_name=task_name,
            step_name=step_name,
            output={"result": runtime.codec.to_primitive(result)},
            duration_ms=duration_ms,
        ),
        operation_id,
    )

    return result
