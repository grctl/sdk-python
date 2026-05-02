from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

import msgspec


class RunStatus(StrEnum):
    pending = "pending"
    scheduled = "scheduled"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
    timed_out = "timed_out"


class RunStateKind(StrEnum):
    start = "start"
    step = "step"
    sleep = "sleep"
    sleep_until = "sleep_until"
    wait_event = "wait_event"
    complete = "complete"
    fail = "fail"
    cancel = "cancel"


class RunInfo(msgspec.Struct, dict=True, omit_defaults=True):
    """Workflow run information.

    Encoded as a dict/map (not array) to match Go's msgpack tag expectations.
    """

    # Required fields
    id: str
    wf_id: str  # Instance ID
    wf_type: str
    status: RunStatus = RunStatus.pending
    worker_name: str | None = None

    # Relationship fields
    parent_wf_id: str | None = None
    parent_wf_type: str | None = None
    parent_run_id: str | None = None

    # Timing fields
    timeout: int | None = None
    created_at: datetime = msgspec.field(default_factory=lambda: datetime.now(UTC))
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    state: RunStateKind | None = None
    history_seq_id: int = 0

    def schedule(self, timestamp: datetime) -> None:
        self.status = RunStatus.scheduled
        self.scheduled_at = timestamp

    def start(self, timestamp: datetime) -> None:
        self.status = RunStatus.running
        self.started_at = timestamp

    def complete(self, timestamp: datetime) -> None:
        self.status = RunStatus.completed
        self.completed_at = timestamp

    def fail(self, timestamp: datetime) -> None:
        self.status = RunStatus.failed
        self.completed_at = timestamp

    def cancel(self, timestamp: datetime) -> None:
        self.status = RunStatus.cancelled
        self.completed_at = timestamp

    def timeout_run(self, timestamp: datetime) -> None:
        self.status = RunStatus.timed_out
        self.completed_at = timestamp
