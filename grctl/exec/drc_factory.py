from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
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
    StepPickedUp,
    StepResult,
    Wait,
)
from grctl.models.directive import NextMessage
from grctl.workflow.workflow import StepInfo

StepHandler = Callable[..., Awaitable[Directive]]


class DrcFactory:
    """Build the directives which describe a completed step and its transition."""

    def __init__(
        self,
        run_info: RunInfo,
        worker_id: str,
        processed_directive: Directive,
        step_infos: Mapping[str, StepInfo],
    ) -> None:
        self._run_info = run_info
        self._worker_id = worker_id
        self._processed_directive = processed_directive
        self._step_infos = step_infos

    def step(self, step_fn: StepHandler) -> Directive:
        """Transition the run to a named workflow step."""
        step_name = self.step_name(step_fn)
        return self._next(
            DirectiveKind.step,
            Step(step_name=step_name, timeout_ms=self._step_infos[step_name].timeout_ms),
        )

    def wait(self, timeout: timedelta | None = None, on_timeout: StepHandler | None = None) -> Directive:
        """Park the run until an event arrives or an optional timeout fires."""
        timeout_step_name = self.step_name(on_timeout) if on_timeout is not None else ""
        return self._next(
            DirectiveKind.wait,
            Wait(
                timeout_ms=int(timeout.total_seconds() * 1000) if timeout else 0,
                timeout_step_name=timeout_step_name,
            ),
        )

    def step_name(self, step_fn: StepHandler) -> str:
        """Resolve a handler function to a registered workflow step name."""
        step_name = getattr(step_fn, "__grctl_step_name__", getattr(step_fn, "__name__", None))
        if not step_name:
            raise ValueError("Step function must have a __name__ attribute.")
        if step_name not in self._step_infos:
            raise ValueError(f"Step handler '{step_name}' is not registered in the workflow")
        return step_name

    def complete(self, result: Any = None) -> Directive:
        """Mark the workflow run complete with its result."""
        return self._next(DirectiveKind.complete, Complete(result=result))

    def fail(self, error: ErrorDetails) -> Directive:
        """Mark the workflow run failed with a structured error."""
        return self._next(DirectiveKind.fail, Fail(error=error))

    def fail_step(self, step_name: str, error: ErrorDetails) -> Directive:
        """Report that a step handler raised. The server records the step as failed and fails the run.

        Distinct from `fail`, which is the handler deciding the run is over: that
        outcome records the step as completed and the run as failed.
        """
        return self._next(DirectiveKind.fail_step, FailStep(step_name=step_name, error=error))

    def step_picked_up(self, step_name: str, timestamp: datetime) -> Directive:
        return Directive(
            id=str(ULID()),
            timestamp=timestamp,
            kind=DirectiveKind.step_picked_up,
            run_info=self._run_info,
            msg=StepPickedUp(
                step_name=step_name,
                worker_id=self._worker_id,
                timestamp=timestamp,
            ),
        )

    def step_result(
        self,
        next_msg_kind: DirectiveKind,
        next_msg: NextMessage,
        timestamp: datetime,
        kv_updates: dict[str, Any] | None = None,
        duration_ms: int = 0,
    ) -> Directive:
        return Directive(
            id=str(ULID()),
            timestamp=timestamp,
            kind=DirectiveKind.step_result,
            run_info=self._run_info,
            msg=StepResult(
                processed_msg_kind=self._processed_directive.kind,
                processed_msg=self._processed_directive.msg,
                worker_id=self._worker_id,
                kv_updates=kv_updates or {},
                next_msg_kind=next_msg_kind,
                next_msg=next_msg,
                duration_ms=duration_ms,
            ),
        )

    def _next(self, kind: DirectiveKind, message: NextMessage) -> Directive:
        """Create the handler return value consumed by Execution.send_step_result.

        This directive is not sent itself: Execution uses only its kind and message
        to create the outbound step-result directive. Reusing the processed
        directive's identity avoids introducing wall-clock or random reads on the
        workflow step path.
        """
        return Directive(
            id=self._processed_directive.id,
            timestamp=self._processed_directive.timestamp,
            kind=kind,
            run_info=self._run_info,
            msg=message,
        )
