from datetime import datetime
from typing import NamedTuple, Protocol

from grctl.models import HistoryEvent, HistoryKind
from grctl.models.history import HistoryEvents


class HistoryWriter(Protocol):
    """Durable store StepHistory appends recorded entries to."""

    async def append(self, event: HistoryEvent) -> None: ...


class HistoryCreateInput(NamedTuple):
    """Everything needed to append one history entry — no identity (wf_id/run_id/worker_id).

    Shared by every writer of step history (the operation history, step lifecycle events, task
    events, ...): each supplies kind/payload/operation_id/timestamp, and StepHistory
    is the one place that stamps identity and persists.
    """

    kind: HistoryKind
    payload: HistoryEvents
    operation_id: str
    timestamp: datetime


class StepHistory:
    """Step-scoped appender over durable history.

    Stamps run/worker identity onto each recorded entry and persists it.

    Deliberately takes the few identifiers it needs (wf_id/run_id/worker_id)
    rather than an Execution — it has no business knowing about handler config,
    kv state, or child tracking.
    """

    def __init__(
        self,
        writer: HistoryWriter,
        wf_id: str,
        run_id: str,
        worker_id: str,
    ) -> None:
        self._writer = writer
        self._wf_id = wf_id
        self._run_id = run_id
        self._worker_id = worker_id

    async def append(self, entry: HistoryCreateInput) -> None:
        event = HistoryEvent(
            wf_id=self._wf_id,
            run_id=self._run_id,
            worker_id=self._worker_id,
            timestamp=entry.timestamp,
            kind=entry.kind,
            msg=entry.payload,
            operation_id=entry.operation_id,
        )
        await self._writer.append(event)
