from __future__ import annotations

import asyncio
import inspect
import random
import time
import traceback
from collections.abc import Awaitable, Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass
from functools import cache, wraps
from typing import TYPE_CHECKING, Any, cast, get_type_hints, overload

from grctl.exec.codec import Codec
from grctl.exec.journal import OperationProgress, Outcome, identify
from grctl.logging_config import get_logger
from grctl.models import ErrorDetails, HistoryKind, TaskCompleted, TaskFailed
from grctl.models.directive import RetryPolicy
from grctl.models.history import HistoryEvents, TaskAttemptFailed, TaskCancelled, TaskStarted

if TYPE_CHECKING:
    from grctl.exec.context import Context

logger = get_logger(__name__)

_ACCEPTABLE_KINDS = frozenset({HistoryKind.task_completed, HistoryKind.task_failed, HistoryKind.task_cancelled})

_DEFAULT_INITIAL_DELAY_MS = 100
_DEFAULT_BACKOFF_MULTIPLIER = 2.0
_DEFAULT_MAX_DELAY_MS = 5000

_current_context: ContextVar[Context | None] = ContextVar("grctl_context", default=None)


def set_current_context(context: Context) -> Token[Context | None]:
    """Make the current workflow step context available to decorated tasks."""
    return _current_context.set(context)


def reset_current_context(token: Token[Context | None]) -> None:
    """Restore the context that was active before a workflow step started."""
    _current_context.reset(token)


@cache
def _return_type(fn: Callable[..., Awaitable[Any]]) -> Any:
    annotation = inspect.signature(fn).return_annotation
    if annotation is inspect.Signature.empty:
        return Any

    def annotation_holder() -> None:
        return None

    annotation_holder.__annotations__["return"] = annotation
    try:
        return get_type_hints(
            annotation_holder,
            globalns=fn.__globals__,
            localns=inspect.getclosurevars(fn).nonlocals,
        )["return"]
    except NameError:
        return Any


@overload
def task[**P, T](fn: Callable[P, Awaitable[T]], /) -> Callable[P, Awaitable[T]]: ...


@overload
def task[**P, T](*, retry_policy: RetryPolicy) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]: ...


def task[**P, T](
    fn: Callable[P, Awaitable[T]] | None = None,
    /,
    *,
    retry_policy: RetryPolicy | None = None,
) -> Callable[P, Awaitable[T]] | Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Journal a function call through the workflow step currently executing.

    Written either bare (`@task`) or with a policy (`@task(retry_policy=...)`). Without a
    policy the function is called once; with one it is retried in place, inside this step,
    until it succeeds or the policy is exhausted. The run is never told about the attempts
    — a task that eventually succeeds is a task that succeeded.
    """

    def decorator(fn: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @wraps(fn)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            context = _current_context.get()
            if context is None:
                raise RuntimeError("A @task function must be called from a workflow step")
            return cast("T", await context.run_task(fn, args, kwargs, retry_policy))

        return wrapped

    if fn is not None:
        return decorator(fn)
    return decorator


@dataclass
class AttemptFailed:
    """An attempt raised, and another one will follow after `delay_ms`."""

    attempt: int
    error: Exception
    stack_trace: str
    delay_ms: int
    duration_ms: int


@dataclass
class Succeeded:
    """The function returned. `attempts` counts every call it took to get here."""

    result: Any
    attempts: int


@dataclass
class Failed:
    """No attempt succeeded and no further one is permitted by the policy."""

    error: Exception
    stack_trace: str
    attempts: int


@dataclass
class Cancelled:
    """The call was cancelled. Cancellation is never an error and is never retried."""

    attempts: int


Terminal = Succeeded | Failed | Cancelled


def _is_retryable(error: Exception, policy: RetryPolicy) -> bool:
    """Decide retryability by exception *name*, not class.

    Policies are authored once and read back over the life of a run; matching on the
    name keeps that decision working after the class is renamed or its module can no
    longer be imported.
    """
    error_type = type(error).__name__
    allowed = policy.retryable_errors is None or error_type in policy.retryable_errors
    denied = policy.non_retryable_errors is not None and error_type in policy.non_retryable_errors
    return allowed and not denied


def _backoff_delay_ms(policy: RetryPolicy, attempt: int) -> int:
    initial = policy.initial_delay_ms or _DEFAULT_INITIAL_DELAY_MS
    multiplier = policy.backoff_multiplier or _DEFAULT_BACKOFF_MULTIPLIER
    max_delay = policy.max_delay_ms or _DEFAULT_MAX_DELAY_MS
    jitter = policy.jitter or 0.0

    delay = min(initial * (multiplier ** (attempt - 1)), max_delay)
    if jitter > 0:
        delay += random.uniform(0, jitter) * delay  # noqa: S311
    return int(delay)


def _error_details(error: Exception, stack_trace: str) -> ErrorDetails:
    return ErrorDetails(
        type=type(error).__name__,
        message=str(error),
        stack_trace=stack_trace,
        qualified_type=f"{type(error).__module__}.{type(error).__qualname__}",
    )


class RetryRunner:
    """Calls a function until the retry policy stops permitting attempts.

    Holds the policy and nothing else — no history, no publishing — so the decision to
    retry can be tested for what it is. Failure is returned, never raised: the caller
    turns the outcome into durable history.

    `already_attempted` seeds the attempt counter with attempts a previous, interrupted
    execution of this same task already made. Delivery is at-least-once, so a step whose
    worker died mid-task is re-delivered and the task runs again; without carrying the
    count forward, a task that kills its worker every time would retry without bound.
    """

    def __init__(
        self,
        fn: Callable[..., Awaitable[Any]],
        policy: RetryPolicy | None,
        already_attempted: int = 0,
    ) -> None:
        self._fn = fn
        self._policy = policy
        self._max_attempts = (policy.max_attempts if policy else None) or 1
        self._already_attempted = already_attempted

    async def run(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        on_attempt_failed: Callable[[AttemptFailed], Awaitable[None]],
    ) -> Terminal:
        attempt = self._already_attempted
        while True:
            attempt += 1
            started_at = time.monotonic()
            try:
                result = await self._fn(*args, **kwargs)
            except asyncio.CancelledError:
                return Cancelled(attempts=attempt)
            except Exception as e:
                stack_trace = traceback.format_exc()
                duration_ms = _elapsed_ms(started_at)
                if not self._may_retry(attempt, e):
                    return Failed(error=e, stack_trace=stack_trace, attempts=attempt)

                delay_ms = _backoff_delay_ms(cast("RetryPolicy", self._policy), attempt)
                await on_attempt_failed(
                    AttemptFailed(
                        attempt=attempt,
                        error=e,
                        stack_trace=stack_trace,
                        delay_ms=delay_ms,
                        duration_ms=duration_ms,
                    )
                )
                await asyncio.sleep(delay_ms / 1000.0)
            else:
                return Succeeded(result=result, attempts=attempt)

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    def _may_retry(self, attempt: int, error: Exception) -> bool:
        if self._policy is None or attempt >= self._max_attempts:
            return False
        return _is_retryable(error, self._policy)


def _elapsed_ms(started_at: float) -> int:
    return int((time.monotonic() - started_at) * 1000)


class Task:
    """Wraps an arbitrary async function as a journal Operation.

    Every call to a `@task` function is one of these. It owns the whole task lifecycle:
    the `task.started` entry, one `task.attempt_failed` per retried attempt, and the
    single terminal entry the journal replays.
    """

    def __init__(  # noqa: PLR0913
        self,
        fn: Callable[..., Awaitable[Any]],
        call_args: tuple[Any, ...],
        call_kwargs: dict[str, Any],
        codec: Codec,
        step_name: str,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._fn = fn
        self._return_type = _return_type(fn)
        self._call_args = call_args
        self._call_kwargs = call_kwargs
        self._codec = codec
        self._step_name = step_name
        self._retry_policy = retry_policy

    @property
    def name(self) -> str:
        return self._fn.__name__  # ty:ignore[unresolved-attribute]

    def operation_id(self, seq: int) -> str:
        return identify(self.name, seq, self.args)

    @property
    def args(self) -> dict[str, Any]:
        """The call's arguments as primitives — recorded in history, and part of its identity."""
        sig = inspect.signature(self._fn)
        bound = sig.bind(*self._call_args, **self._call_kwargs)
        bound.apply_defaults()
        return {name: self._codec.to_primitive(value) for name, value in bound.arguments.items()}

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return _ACCEPTABLE_KINDS

    async def perform(self, progress: OperationProgress) -> Outcome:
        started_at = time.monotonic()
        await progress.record(
            HistoryKind.task_started,
            TaskStarted(
                task_id=progress.operation_id,
                task_name=self.name,
                args=self.args,
                step_name=self._step_name,
            ),
        )

        runner = RetryRunner(
            self._fn,
            self._retry_policy,
            already_attempted=progress.count(HistoryKind.task_attempt_failed),
        )
        outcome = await runner.run(
            self._call_args,
            self._call_kwargs,
            lambda event: self._record_attempt_failed(progress, event, runner.max_attempts),
        )
        return self._terminal_outcome(progress.operation_id, outcome, _elapsed_ms(started_at))

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> Any:
        if kind == HistoryKind.task_cancelled:
            raise asyncio.CancelledError

        if kind == HistoryKind.task_failed:
            if not isinstance(payload, TaskFailed):
                raise TypeError(f"Expected TaskFailed payload, got {type(payload)}")
            raise RuntimeError(f"{payload.error.type}: {payload.error.message}")

        if not isinstance(payload, TaskCompleted):
            raise TypeError(f"Expected TaskCompleted payload, got {type(payload)}")
        return self._codec.from_primitive(payload.output["result"], self._return_type)

    async def _record_attempt_failed(
        self, progress: OperationProgress, event: AttemptFailed, max_attempts: int
    ) -> None:
        logger.warning(
            f"Task {self.name} attempt {event.attempt}/{max_attempts} failed in step "
            f"{self._step_name}: {event.error!r} — retrying in {event.delay_ms}ms"
        )
        await progress.record(
            HistoryKind.task_attempt_failed,
            TaskAttemptFailed(
                task_id=progress.operation_id,
                task_name=self.name,
                step_name=self._step_name,
                attempt=event.attempt,
                max_attempts=max_attempts,
                error=_error_details(event.error, event.stack_trace),
                next_retry_delay_ms=event.delay_ms,
                duration_ms=event.duration_ms,
            ),
        )

    def _terminal_outcome(self, operation_id: str, outcome: Terminal, duration_ms: int) -> Outcome:
        """Turn the runner's verdict into the one entry that resolves this operation on replay."""
        match outcome:
            case Succeeded(result=result):
                return HistoryKind.task_completed, TaskCompleted(
                    task_id=operation_id,
                    task_name=self.name,
                    output={"result": self._codec.to_primitive(result)},
                    step_name=self._step_name,
                    duration_ms=duration_ms,
                )
            case Failed(error=error, stack_trace=stack_trace, attempts=attempts):
                logger.warning(
                    f"Task {self.name} failed in step {self._step_name} after {attempts} attempt(s): {error!r}"
                )
                return HistoryKind.task_failed, TaskFailed(
                    task_id=operation_id,
                    task_name=self.name,
                    step_name=self._step_name,
                    error=_error_details(error, stack_trace),
                    duration_ms=duration_ms,
                )
            case Cancelled():
                logger.warning(f"Task {self.name} cancelled in step {self._step_name}")
                return HistoryKind.task_cancelled, TaskCancelled(
                    task_id=operation_id,
                    task_name=self.name,
                    step_name=self._step_name,
                    duration_ms=duration_ms,
                )
