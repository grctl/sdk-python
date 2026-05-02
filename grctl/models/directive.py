"""Custom msgpack encoding/decoding for Directive types.

This module provides msgpack serialization for Directive messages with a compact wire format
that matches the Go server implementation.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any

import msgspec

from grctl.logging_config import get_logger
from grctl.models.common import ErrorDetails
from grctl.models.run_info import RunInfo

logger = get_logger(__name__)


class RetryPolicy(msgspec.Struct, omit_defaults=True):
    max_attempts: int | None = None
    initial_delay_ms: int | None = None
    backoff_multiplier: float | None = None
    max_delay_ms: int | None = None
    jitter: float | None = None
    retryable_errors: list[str] | None = None
    non_retryable_errors: list[str] | None = None


class Start(msgspec.Struct):
    """Request to start workflow execution."""

    input: Any | None = None
    timeout_ms: int | None = 3_000  # 3 seconds in nanoseconds (Go time.Duration)


class Cancel(msgspec.Struct):
    """Request to cancel a running workflow."""

    reason: str | None = None


class Event(msgspec.Struct):
    """Request to emit an event to a running workflow."""

    event_name: str
    payload: Any | None = None


class Step(msgspec.Struct):
    """Request to execute a specific step in a workflow."""

    step_name: str
    timeout_ms: int | None = 3_000  # 3 seconds in nanoseconds (Go time.Duration)


class WaitEvent(msgspec.Struct):
    """Worker directive to wait for events."""

    timeout_ms: int = 3000
    timeout_step_name: str | None = None


class Sleep(msgspec.Struct):
    """Worker directive to sleep for duration."""

    next_step_name: str
    duration_ms: int = 3000


class SleepUntil(msgspec.Struct):
    """Worker directive to sleep until timestamp."""

    until: datetime
    next_step_name: str


class Complete(msgspec.Struct):
    """Worker directive to mark workflow complete."""

    result: Any


class Fail(msgspec.Struct):
    """Worker directive to mark workflow failed."""

    error: ErrorDetails


class DirectiveKind(StrEnum):
    start = "start"
    cancel = "cancel"
    terminate = "terminate"
    complete = "complete"
    fail = "fail"
    step = "step"
    event = "event"
    wait_event = "wait_event"
    sleep = "sleep"
    sleep_until = "sleep_until"
    step_result = "step_result"


class StepResult(msgspec.Struct):
    """Worker directive to mark step complete."""

    processed_msg_kind: DirectiveKind
    # Any because processed_msg and next_msg are type-erased at wire level;
    # the _kind fields carry the type info needed for deserialization.
    processed_msg: Any
    worker_id: str
    kv_updates: dict[str, Any]
    next_msg_kind: DirectiveKind
    next_msg: Any
    duration_ms: int = 0


DirectiveMessage = Start | Cancel | Event | Complete | Fail | Step | WaitEvent | Sleep | SleepUntil | StepResult


# Factory map for kind-based deserialization
directive_factories: dict[str, type[DirectiveMessage]] = {
    "start": Start,
    "cancel": Cancel,
    "event": Event,
    "complete": Complete,
    "fail": Fail,
    "step": Step,
    "wait_event": WaitEvent,
    "sleep": Sleep,
    "sleep_until": SleepUntil,
    "step_result": StepResult,
}


class Directive(msgspec.Struct):
    id: str
    timestamp: datetime
    kind: DirectiveKind
    run_info: RunInfo
    msg: DirectiveMessage
    attempt: int = 0
    kv_revs: dict[str, Any] | None = None


class DirectiveWire(msgspec.Struct, omit_defaults=True):
    """Wire format for Directive with compact field names matching Go server.

    Encoded as a dict/map (not array) to match Go's msgpack tag expectations.
    """

    id: str
    k: str  # kind
    m: bytes  # message
    r: RunInfo  # run_info
    t: datetime  # timestamp
    a: int = 0  # attempt
    kv: dict[str, Any] | None = None  # kv_revs


def directive_encoder(directive: Directive, enc_hook: Any = None) -> bytes:
    if directive.msg is None:
        raise ValueError("Directive message cannot be None")

    msg_bytes = msgspec.msgpack.encode(directive.msg, enc_hook=enc_hook)

    wire = DirectiveWire(
        id=directive.id,
        k=directive.kind,
        m=msg_bytes,
        r=directive.run_info,
        t=directive.timestamp,
        a=directive.attempt,
        kv=directive.kv_revs,
    )

    return msgspec.msgpack.encode(wire)


def directive_decoder(data: bytes) -> Directive:
    wire = msgspec.msgpack.decode(data, type=DirectiveWire)

    factory = directive_factories.get(wire.k)
    if factory is None:
        raise ValueError(f"Unknown directive kind: {wire.k}")

    msg = msgspec.msgpack.decode(wire.m, type=factory)

    # Convert kind string to DirectiveKind enum
    kind_enum = DirectiveKind(wire.k)

    return Directive(
        id=wire.id,
        kind=kind_enum,
        attempt=wire.a,
        kv_revs=wire.kv or {},
        msg=msg,
        run_info=wire.r,
        timestamp=wire.t,
    )
