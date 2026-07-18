from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from ulid import ULID

from grctl.models import (
    Complete,
    Directive,
    DirectiveKind,
    ErrorDetails,
    Fail,
    FailStep,
    RunInfo,
    Step,
    StepResult,
    Wait,
)
from grctl.worker.kv_store import KVStore
from grctl.workflow.workflow import HandlerConfig

StepHandler = Callable[..., Awaitable[Directive]]


class NextDirectiveBuilder:
    """Builder for creating step transition directives.

    Allows both ctx.next.step(step_func) and ctx.next.complete(result) syntax.
    """

    def __init__(
        self,
        run: RunInfo,
        worker_id: str,
        kv_store: KVStore,
        current_directive: Directive,
        step_configs: dict[str, HandlerConfig] | None = None,
    ) -> None:
        self._run = run
        self._worker_id = worker_id
        self._kv_store = kv_store
        self._current_directive = current_directive
        self._step_configs = step_configs or {}

    def step(self, step_fn: StepHandler) -> Directive:
        step_name = getattr(step_fn, "__name__", None)
        if step_name is None:
            raise ValueError("Step function must have a __name__ attribute.")

        config = self._step_configs.get(step_name)
        timeout_ms = int(config.timeout.total_seconds() * 1000) if config and config.timeout else None

        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._kv_store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.step,
            next_msg=Step(
                step_name=step_name,
                timeout_ms=timeout_ms,
            ),
        )

        return Directive(
            id=str(ULID()), kind=DirectiveKind.step_result, run_info=self._run, timestamp=datetime.now(UTC), msg=res
        )

    def wait(self, timeout: timedelta | None = None, on_timeout: StepHandler | None = None) -> Directive:
        timeout_step_name = getattr(on_timeout, "__name__", "") if on_timeout is not None else ""
        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._kv_store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.wait,
            next_msg=Wait(
                timeout_ms=int(timeout.total_seconds() * 1000) if timeout else 0,
                timeout_step_name=timeout_step_name,
            ),
        )

        return Directive(
            id=str(ULID()), kind=DirectiveKind.step_result, run_info=self._run, timestamp=datetime.now(UTC), msg=res
        )

    def complete(self, result: Any) -> Directive:
        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._kv_store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.complete,
            next_msg=Complete(
                result=result,
            ),
        )

        return Directive(
            id=str(ULID()), kind=DirectiveKind.step_result, run_info=self._run, timestamp=datetime.now(UTC), msg=res
        )

    def fail_step(self, step_name: str, error: ErrorDetails) -> Directive:
        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._kv_store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.fail_step,
            next_msg=FailStep(
                step_name=step_name,
                error=error,
            ),
        )

        return Directive(
            id=str(ULID()),
            kind=DirectiveKind.step_result,
            run_info=self._run,
            timestamp=datetime.now(UTC),
            msg=res,
        )

    def fail(self, error: ErrorDetails) -> Directive:
        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._kv_store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.fail,
            next_msg=Fail(
                error=error,
            ),
        )

        return Directive(
            id=str(ULID()),
            kind=DirectiveKind.step_result,
            run_info=self._run,
            timestamp=datetime.now(UTC),
            msg=res,
        )
