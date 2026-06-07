from datetime import datetime
from enum import StrEnum
from typing import Any

import msgspec

from grctl.models.run_info import RunInfo


class CmdKind(StrEnum):
    run_start = "run.start"
    run_cancel = "run.cancel"
    run_describe = "run.describe"
    run_terminate = "run.terminate"
    run_event = "run.event"
    worker_register = "worker.register"
    worker_terminate_run = "worker.terminate_run"


class StartCmd(msgspec.Struct):
    """Request to start workflow execution."""

    run_info: RunInfo
    input: Any | None


class CancelCmd(msgspec.Struct):
    """Request to cancel a running workflow."""

    wf_id: str
    reason: str | None


class DescribeCmd(msgspec.Struct):
    """Request to describe a workflow run."""

    wf_id: str


class EventCmd(msgspec.Struct):
    """Request to emit an event to a running workflow."""

    wf_id: str
    event_name: str
    payload: Any | None


class TerminateCmd(msgspec.Struct):
    """Request to terminate a running workflow."""

    wf_id: str
    reason: str | None


class WorkerTerminateRunCmd(msgspec.Struct):
    """Server→worker signal to cancel a specific in-flight run."""

    run_id: str


class EventDef(msgspec.Struct, kw_only=True):
    """Per-event timeout config carried through registration."""

    name: str
    timeout_ms: int = 0


class WorkflowTypeDef(msgspec.Struct):
    """Structural definition of one workflow type reported at registration.

    Field names and order mirror the Go WorkflowTypeDef msgpack tags.
    """

    type: str
    start_step: str
    steps: list[str]
    events: list[EventDef]
    queries: list[str]
    start_step_timeout_ms: int = 0


class RegisterCmd(msgspec.Struct):
    """Worker startup sync of its workflow type catalog to the server."""

    worker_id: str
    types: list[WorkflowTypeDef]


type CommandMessage = StartCmd | EventCmd | CancelCmd | DescribeCmd | TerminateCmd | RegisterCmd | WorkerTerminateRunCmd


class Command(msgspec.Struct):
    id: str
    kind: CmdKind
    timestamp: datetime
    msg: CommandMessage
    sender_id: str = ""


# Factory map for kind-based deserialization
command_factories: dict[str, type] = {
    "run.start": StartCmd,
    "run.cancel": CancelCmd,
    "run.describe": DescribeCmd,
    "run.terminate": TerminateCmd,
    "run.event": EventCmd,
    "worker.register": RegisterCmd,
    "worker.terminate_run": WorkerTerminateRunCmd,
}


class CommandWire(msgspec.Struct):
    """Wire format for Command with compact field names matching Go server.

    Encoded as a dict/map (not array) to match Go's msgpack tag expectations.
    """

    id: str
    k: CmdKind
    m: bytes
    t: datetime
    s: str = ""


def command_encoder(cmd: Command) -> bytes:
    """Encode command to msgpack with compact wire format."""
    if cmd.msg is None:
        raise ValueError("Command message cannot be None")
    if cmd.sender_id == "":
        raise ValueError("Command sender ID cannot be empty")

    msg_bytes = msgspec.msgpack.encode(cmd.msg)

    wire = CommandWire(
        id=cmd.id,
        k=cmd.kind,
        m=msg_bytes,
        t=cmd.timestamp,
        s=cmd.sender_id,
    )

    return msgspec.msgpack.encode(wire)


def command_decoder(data: bytes) -> Command:
    """Decode msgpack to command."""
    wire = msgspec.msgpack.decode(data, type=CommandWire)

    factory = command_factories.get(wire.k)
    if factory is None:
        raise ValueError(f"Unknown command kind: {wire.k}")

    msg = msgspec.msgpack.decode(wire.m, type=factory)

    return Command(
        id=wire.id,
        kind=wire.k,
        msg=msg,  # ty:ignore[invalid-argument-type]
        timestamp=wire.t,
        sender_id=wire.s,
    )
