from datetime import UTC, datetime
from typing import Any

import msgspec
from nats.aio.client import Client as NATSClient
from ulid import ULID

from grctl.models import (
    CancelCmd,
    CmdKind,
    Command,
    EventCmd,
    GrctlAPIResponse,
    RunInfo,
    StartCmd,
    TerminateCmd,
    command_encoder,
)
from grctl.models.command import CommandMessage
from grctl.nats.codec import CodecRegistry
from grctl.nats.manifest import NatsManifest

_REQUEST_TIMEOUT_SECONDS = 5.0


class NatsWorkflowAPI:
    """Run-scoped calls to the server over NATS request/reply.

    Builds the Command envelope and routes it to the run's api subject; the
    domain passes only its intent (run + payload + who is asking).
    """

    def __init__(self, nc: NATSClient, manifest: NatsManifest, codec: CodecRegistry) -> None:
        self._nc = nc
        self._manifest = manifest
        self._codec = codec

    async def start_run(self, run_info: RunInfo, input: Any, sender_id: str) -> GrctlAPIResponse:  # noqa: A002
        return await self._request(run_info, CmdKind.run_start, StartCmd(run_info=run_info, input=input), sender_id)

    async def send_event(
        self, run_info: RunInfo, event_name: str, payload: Any, sender_id: str
    ) -> GrctlAPIResponse:
        msg = EventCmd(wf_id=run_info.wf_id, event_name=event_name, payload=payload)
        return await self._request(run_info, CmdKind.run_event, msg, sender_id)

    async def cancel_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> GrctlAPIResponse:
        msg = CancelCmd(wf_id=run_info.wf_id, reason=reason)
        return await self._request(run_info, CmdKind.run_cancel, msg, sender_id)

    async def terminate_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> GrctlAPIResponse:
        msg = TerminateCmd(wf_id=run_info.wf_id, reason=reason)
        return await self._request(run_info, CmdKind.run_terminate, msg, sender_id)

    async def _request(self, run_info: RunInfo, kind: CmdKind, msg: CommandMessage, sender_id: str) -> GrctlAPIResponse:
        cmd = Command(id=str(ULID()), kind=kind, timestamp=datetime.now(UTC), msg=msg, sender_id=sender_id)
        subject = self._manifest.api_subject(wf_id=run_info.wf_id)
        data = command_encoder(cmd, enc_hook=self._codec.enc_hook)
        reply = await self._nc.request(subject, data, timeout=_REQUEST_TIMEOUT_SECONDS)
        return msgspec.msgpack.decode(reply.data, type=GrctlAPIResponse)
