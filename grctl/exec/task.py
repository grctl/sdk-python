import inspect
import time
import traceback
from collections.abc import Awaitable, Callable
from typing import Any

from grctl.exec.journal import Outcome
from grctl.models import ErrorDetails, HistoryKind, TaskCompleted, TaskFailed
from grctl.models.history import HistoryEvents
from grctl.nats.codec import MsgspecCodec

_ACCEPTABLE_KINDS = frozenset({HistoryKind.task_completed, HistoryKind.task_failed})


class Task:
    """Wraps an arbitrary async function as a journal Operation."""

    def __init__(
        self,
        fn: Callable[..., Awaitable[Any]],
        call_args: tuple[Any, ...],
        call_kwargs: dict[str, Any],
    ) -> None:
        self._fn = fn
        self._call_args = call_args
        self._call_kwargs = call_kwargs
        self._codec = MsgspecCodec()

    @property
    def name(self) -> str:
        return self._fn.__name__  # ty:ignore[unresolved-attribute]

    @property
    def args(self) -> dict[str, Any]:
        sig = inspect.signature(self._fn)
        bound = sig.bind(*self._call_args, **self._call_kwargs)
        bound.apply_defaults()
        return {name: self._codec.to_primitive(value) for name, value in bound.arguments.items()}

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return _ACCEPTABLE_KINDS

    async def perform(self) -> Outcome:
        started_at = time.monotonic()
        try:
            result = await self._fn(*self._call_args, **self._call_kwargs)
        except Exception as e:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            payload = TaskFailed(
                task_id=self.name,
                task_name=self.name,
                step_name="",
                error=ErrorDetails(type=type(e).__name__, message=str(e), stack_trace=traceback.format_exc()),
                duration_ms=duration_ms,
            )
            return HistoryKind.task_failed, payload

        duration_ms = int((time.monotonic() - started_at) * 1000)
        payload = TaskCompleted(
            task_id=self.name,
            task_name=self.name,
            output={"result": self._codec.to_primitive(result)},
            step_name="",
            duration_ms=duration_ms,
        )
        return HistoryKind.task_completed, payload

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> Any:
        if kind == HistoryKind.task_failed:
            if not isinstance(payload, TaskFailed):
                raise TypeError(f"Expected TaskFailed payload, got {type(payload)}")
            raise RuntimeError(f"{payload.error.type}: {payload.error.message}")

        if not isinstance(payload, TaskCompleted):
            raise TypeError(f"Expected TaskCompleted payload, got {type(payload)}")
        return payload.output["result"]
