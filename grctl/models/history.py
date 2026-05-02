from datetime import datetime
from enum import StrEnum
from typing import Any

import msgspec

from grctl.models.common import ErrorDetails


class HistoryKind(StrEnum):
    run_scheduled = "run.scheduled"
    run_started = "run.started"
    run_completed = "run.completed"
    run_failed = "run.failed"
    run_cancel_scheduled = "run.cancel_scheduled"
    run_cancelled = "run.cancelled"
    run_timeout = "run.timeout"
    wait_event_started = "wait_event.started"
    event_received = "event.received"
    step_started = "step.started"
    step_completed = "step.completed"
    step_failed = "step.failed"
    step_cancelled = "step.cancelled"
    step_timeout = "step.timeout"
    task_started = "task.started"
    task_completed = "task.completed"
    task_failed = "task.failed"
    task_attempt_failed = "task.attempt_failed"
    task_cancelled = "task.cancelled"
    timestamp_recorded = "timestamp.recorded"
    random_recorded = "random.recorded"
    uuid_recorded = "uuid.recorded"
    sleep_recorded = "sleep.recorded"
    child_started = "child.started"
    parent_event_sent = "parent.event_sent"


class RunScheduled(msgspec.Struct):
    """Workflow run queued for execution."""


class RunStarted(msgspec.Struct):
    """Workflow execution began."""

    input: Any = None


class RunCompleted(msgspec.Struct):
    """Workflow finished successfully."""

    result: Any
    duration_ms: int


class RunFailed(msgspec.Struct):
    """Workflow execution failed."""

    error: ErrorDetails
    duration_ms: int


class RunCancelScheduled(msgspec.Struct):
    """Workflow cancellation has been scheduled."""


class RunCancelled(msgspec.Struct):
    """Workflow execution was cancelled."""

    reason: str
    duration_ms: int


class RunTimeout(msgspec.Struct):
    """Workflow execution timed out."""

    duration_ms: int  # Actual time before timeout


class StepStarted(msgspec.Struct):
    """Step execution began."""

    step_name: str


class StepCompleted(msgspec.Struct):
    """Step finished successfully."""

    step_name: str
    duration_ms: int


class StepFailed(msgspec.Struct):
    """Step execution failed."""

    step_name: str
    error: ErrorDetails
    duration_ms: int


class StepCancelled(msgspec.Struct):
    """Step execution was cancelled."""

    step_name: str


class StepTimeout(msgspec.Struct):
    """Step execution timed out."""

    step_name: str
    duration_ms: int


class WaitEventStarted(msgspec.Struct):
    """Wait for event started."""


class EventReceived(msgspec.Struct):
    """Event received during wait."""

    event_name: str
    payload: Any


class TaskStarted(msgspec.Struct):
    """Task execution began."""

    task_id: str  # Deterministic ID: "task_name:args_hash"
    task_name: str  # Function name
    args: dict[str, Any]  # Task arguments
    step_name: str  # Which step called this task


class TaskCompleted(msgspec.Struct):
    """Task finished successfully."""

    task_id: str
    task_name: str
    output: dict[str, Any]  # Always {"result": <primitive>}
    step_name: str
    duration_ms: int


class TaskFailed(msgspec.Struct):
    """Task execution failed."""

    task_id: str
    task_name: str
    step_name: str
    error: ErrorDetails
    duration_ms: int


class TaskAttemptFailed(msgspec.Struct):
    """A task retry attempt failed (will be retried)."""

    task_id: str
    task_name: str
    step_name: str
    attempt: int
    max_attempts: int
    error: ErrorDetails
    next_retry_delay_ms: int
    duration_ms: int


class TaskCancelled(msgspec.Struct):
    """Task execution was cancelled."""

    task_id: str
    task_name: str
    step_name: str
    duration_ms: int


class TimestampRecorded(msgspec.Struct):
    """Recorded timestamp for deterministic replay."""

    value: datetime


class RandomRecorded(msgspec.Struct):
    """Recorded random value for deterministic replay."""

    value: float


class UuidRecorded(msgspec.Struct):
    """Recorded UUID for deterministic replay."""

    value: str


class SleepRecorded(msgspec.Struct):
    """Recorded sleep duration for deterministic replay."""

    duration_ms: int


class ChildWorkflowStarted(msgspec.Struct):
    """Recorded child workflow start for deterministic replay."""

    run_id: str
    wf_type: str
    wf_id: str
    input: Any | None = None


class ParentEventSent(msgspec.Struct):
    """Recorded parent event send for deterministic replay."""

    event_name: str
    payload: Any
    parent_wf_type: str
    parent_wf_id: str


RunEvents = RunCancelScheduled | RunCancelled | RunCompleted | RunFailed | RunScheduled | RunStarted | RunTimeout
WaitEvents = WaitEventStarted | EventReceived
StepEvents = StepStarted | StepCompleted | StepFailed | StepCancelled | StepTimeout
TaskEvents = TaskStarted | TaskCompleted | TaskFailed | TaskAttemptFailed | TaskCancelled
DeterministicEvents = TimestampRecorded | RandomRecorded | UuidRecorded | SleepRecorded
HistoryEvents = (
    RunCancelScheduled
    | RunCancelled
    | RunCompleted
    | RunFailed
    | RunScheduled
    | RunStarted
    | RunTimeout
    | StepStarted
    | StepCompleted
    | StepFailed
    | StepCancelled
    | StepTimeout
    | WaitEventStarted
    | EventReceived
    | TaskStarted
    | TaskCompleted
    | TaskFailed
    | TaskAttemptFailed
    | TaskCancelled
    | TimestampRecorded
    | RandomRecorded
    | UuidRecorded
    | SleepRecorded
    | ChildWorkflowStarted
    | ParentEventSent
)


class HistoryEvent(msgspec.Struct):
    """Envelope for history messages."""

    wf_id: str
    run_id: str
    worker_id: str
    timestamp: datetime
    kind: HistoryKind
    msg: HistoryEvents
    operation_id: str = ""


# Factory map for kind-based deserialization
history_factories: dict[str, type] = {
    "run.scheduled": RunScheduled,
    "run.started": RunStarted,
    "run.completed": RunCompleted,
    "run.failed": RunFailed,
    "run.cancel_scheduled": RunCancelScheduled,
    "run.cancelled": RunCancelled,
    "run.timeout": RunTimeout,
    "wait_event.started": WaitEventStarted,
    "event.received": EventReceived,
    "step.started": StepStarted,
    "step.completed": StepCompleted,
    "step.failed": StepFailed,
    "step.cancelled": StepCancelled,
    "step.timeout": StepTimeout,
    "task.started": TaskStarted,
    "task.completed": TaskCompleted,
    "task.failed": TaskFailed,
    "task.attempt_failed": TaskAttemptFailed,
    "task.cancelled": TaskCancelled,
    "timestamp.recorded": TimestampRecorded,
    "random.recorded": RandomRecorded,
    "uuid.recorded": UuidRecorded,
    "sleep.recorded": SleepRecorded,
    "child.started": ChildWorkflowStarted,
    "parent.event_sent": ParentEventSent,
}


class HistoryWire(msgspec.Struct):
    w: str
    r: str
    wo: str
    ts: datetime
    k: str
    m: bytes
    o: str = ""


def history_encoder(event: HistoryEvent, enc_hook: Any = None) -> bytes:
    """Encode history event to msgpack as array: [kind, msg, run_id, timestamp, wf_id, worker_id]."""
    if event.msg is None:
        raise ValueError("HistoryEvent message cannot be None")

    wire = HistoryWire(
        w=event.wf_id,
        r=event.run_id,
        wo=event.worker_id,
        ts=event.timestamp,
        k=event.kind,
        m=msgspec.msgpack.encode(event.msg, enc_hook=enc_hook),
        o=event.operation_id,
    )

    return msgspec.msgpack.encode(wire)


def history_decoder(data: bytes) -> HistoryEvent:
    """Decode msgpack array to history event."""
    wire = msgspec.msgpack.decode(data, type=HistoryWire)

    # Get factory for this kind
    factory = history_factories.get(wire.k)
    if factory is None:
        raise ValueError(f"Unknown history event kind: {wire.k}")

    # Decode msg from raw msgpack
    msg = msgspec.msgpack.decode(wire.m, type=factory)

    # Convert kind string to HistoryKind enum
    kind_enum = HistoryKind(wire.k)

    return HistoryEvent(
        kind=kind_enum,
        msg=msg,  # ty:ignore[invalid-argument-type]
        run_id=wire.r,
        timestamp=wire.ts,
        wf_id=wire.w,
        worker_id=wire.wo,
        operation_id=wire.o,
    )
