from __future__ import annotations

from datetime import datetime
from typing import Any

from ulid import ULID

from grctl.models import (
    Directive,
    DirectiveKind,
    RunInfo,
    StepPickedUp,
    StepResult,
)
from grctl.models.directive import NextMessage


class DrcFactory:
    def __init__(
        self,
        run_info: RunInfo,
        worker_id: str,
        processed_directive: Directive,
    ) -> None:
        self._run_info = run_info
        self._worker_id = worker_id
        self._processed_directive = processed_directive

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
