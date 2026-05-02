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


type CommandMessage = StartCmd | EventCmd | CancelCmd | DescribeCmd | TerminateCmd


class Command(msgspec.Struct):
    id: str
    kind: CmdKind
    timestamp: datetime
    msg: CommandMessage


# Factory map for kind-based deserialization
command_factories: dict[str, type] = {
    "run.start": StartCmd,
    "run.cancel": CancelCmd,
    "run.describe": DescribeCmd,
    "run.terminate": TerminateCmd,
    "run.event": EventCmd,
}


class CommandWire(msgspec.Struct):
    """Wire format for Command with compact field names matching Go server.

    Encoded as a dict/map (not array) to match Go's msgpack tag expectations.
    """

    id: str
    k: CmdKind
    m: bytes
    t: datetime


def command_encoder(cmd: Command) -> bytes:
    """Encode command to msgpack with compact wire format."""
    if cmd.msg is None:
        raise ValueError("Command message cannot be None")

    msg_bytes = msgspec.msgpack.encode(cmd.msg)

    wire = CommandWire(
        id=cmd.id,
        k=cmd.kind,
        m=msg_bytes,
        t=cmd.timestamp,
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
    )
