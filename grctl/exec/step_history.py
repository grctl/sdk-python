from datetime import datetime
from typing import NamedTuple

from grctl.models import HistoryEvent, HistoryKind
from grctl.models.history import HistoryEvents
from grctl.nats.history import HistoryStore


class HistoryCreateInput(NamedTuple):
    """Everything needed to append one history entry — no identity (wf_id/run_id/worker_id).

    Shared by every writer of step history (the journal, step lifecycle events, task
    events, ...): each supplies kind/payload/operation_id/timestamp, and StepHistory
    is the one place that stamps identity and persists.
    """

    kind: HistoryKind
    payload: HistoryEvents
    operation_id: str
    timestamp: datetime


class StepHistory:
    """Step-scoped view over durable history.

    Fetches the replay prefix for one step and appends newly recorded entries for it.

    Deliberately takes the few identifiers it needs (wf_id/run_id/worker_id,
    the seq to replay from) rather than an Execution — it has no business
    knowing about handler config, kv state, or child tracking.
    """

    def __init__(
        self,
        store: HistoryStore,
        wf_id: str,
        run_id: str,
        worker_id: str,
        history_seq_id: int,
    ) -> None:
        self._store = store
        self._wf_id = wf_id
        self._run_id = run_id
        self._worker_id = worker_id
        self._history_seq_id = history_seq_id

    async def fetch(self) -> list[HistoryEvent]:
        return await self._store.fetch_step_history(self._wf_id, self._run_id, self._history_seq_id)

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
        await self._store.append(event)
