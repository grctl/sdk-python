from datetime import UTC, datetime
from typing import Any

import msgspec
from ulid import ULID

from grctl.models import (
    CancelCmd,
    CmdKind,
    Command,
    DescribeCmd,
    EventCmd,
    GrctlAPIResponse,
    RunInfo,
    StartCmd,
    TerminateCmd,
    command_encoder,
)
from grctl.models.command import CommandMessage
from grctl.models.errors import error_for_code
from grctl.nats.codec import MsgspecCodec
from grctl.nats.manifest import manifest
from grctl.nats.nats_client import CoreRequestClient
from grctl.settings import get_settings


class NatsWorkflowAPI:
    """Workflow calls to the server over NATS request/reply.

    Builds the Command envelope and routes it to the workflow's api subject; the
    domain passes only its intent (target + payload + who is asking).
    """

    def __init__(self, nc: CoreRequestClient, codec: MsgspecCodec) -> None:
        self._nc = nc
        self._codec = codec
        self._settings = get_settings()

    async def start_run(self, run_info: RunInfo, input: Any, sender_id: str) -> None:  # noqa: A002
        msg = StartCmd(run_info=run_info, input=input)
        await self._request(run_info.wf_id, CmdKind.run_start, msg, sender_id)

    async def send_event(self, run_info: RunInfo, event_name: str, payload: Any, sender_id: str) -> None:
        msg = EventCmd(wf_id=run_info.wf_id, event_name=event_name, payload=payload)
        await self._request(run_info.wf_id, CmdKind.run_event, msg, sender_id)

    async def cancel_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None:
        msg = CancelCmd(wf_id=run_info.wf_id, reason=reason)
        await self._request(run_info.wf_id, CmdKind.run_cancel, msg, sender_id)

    async def terminate_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None:
        msg = TerminateCmd(wf_id=run_info.wf_id, reason=reason)
        await self._request(run_info.wf_id, CmdKind.run_terminate, msg, sender_id)

    async def describe_run(self, wf_id: str, sender_id: str) -> RunInfo:
        response = await self._request(wf_id, CmdKind.run_describe, DescribeCmd(wf_id=wf_id), sender_id)
        return msgspec.msgpack.decode(response.payload, type=RunInfo)

    async def _request(self, wf_id: str, kind: CmdKind, msg: CommandMessage, sender_id: str) -> GrctlAPIResponse:
        """Send a command and return the reply, raising if the server rejected it.

        The envelope stops here: callers get domain values and domain errors,
        never the wire representation of either.
        """
        cmd = Command(id=str(ULID()), kind=kind, timestamp=datetime.now(UTC), msg=msg, sender_id=sender_id)
        subject = manifest.api_subject(wf_id=wf_id)
        data = command_encoder(cmd, enc_hook=self._codec.enc_hook)
        reply = await self._nc.request(subject, data, timeout=self._settings.nats_request_timeout)
        response = msgspec.msgpack.decode(reply.data, type=GrctlAPIResponse)
        if not response.success:
            error = response.error
            raise error_for_code(error.code if error else 0, error.message if error else "unknown error")
        return response
