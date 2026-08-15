import asyncio
from datetime import UTC, datetime

import msgspec
from ulid import ULID

from grctl.logging_config import get_logger
from grctl.models import (
    CmdKind,
    Command,
    GrctlAPIResponse,
    RegisterCmd,
    WorkflowTypeDef,
    command_encoder,
)
from grctl.nats.codec import MsgspecCodec
from grctl.nats.manifest import manifest
from grctl.nats.nats_client import CoreRequestClient
from grctl.settings import get_settings

logger = get_logger(__name__)


class NatsWorkerAPI:
    """Worker-scoped calls to the server (register, ...) over NATS request/reply.

    Builds the Command envelope and routes it; the domain passes only its intent.
    """

    def __init__(self, nc: CoreRequestClient, codec: MsgspecCodec) -> None:
        self._nc = nc
        self._codec = codec

    async def register_worker(self, worker_id: str, catalog: list[WorkflowTypeDef]) -> GrctlAPIResponse:
        """Register this worker and the workflow types it serves with the server."""
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.worker_register,
            timestamp=datetime.now(UTC),
            msg=RegisterCmd(worker_id=worker_id, types=catalog),
            sender_id=worker_id,
        )
        return await self._request(cmd)

    async def _request(self, cmd: Command) -> GrctlAPIResponse:
        """Send a command and await the decoded reply, retrying transport failures.

        A server-side rejection comes back as an unsuccessful GrctlAPIResponse and
        is returned to the caller; only transport failures are retried, and the
        last one propagates once attempts are exhausted.
        """
        settings = get_settings()
        max_attempts = settings.nats_worker_registration_max_attempts
        for attempt in range(1, max_attempts + 1):
            try:
                reply = await self._send(cmd)
            except Exception as exc:
                logger.warning("Worker API request '%s' attempt %d/%d failed: %s", cmd.kind, attempt, max_attempts, exc)
                if attempt == max_attempts:
                    raise
                await asyncio.sleep(settings.nats_worker_registration_retry_base_delay_seconds * attempt)
                continue
            return msgspec.msgpack.decode(reply, type=GrctlAPIResponse)

        raise AssertionError("unreachable")  # loop either returns or raises

    async def _send(self, cmd: Command) -> bytes:
        subject = manifest.worker_command_subject()
        data = command_encoder(cmd, enc_hook=self._codec.enc_hook)
        msg = await self._nc.request(subject, data, timeout=get_settings().nats_request_timeout)
        return msg.data
