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

logger = get_logger(__name__)

_REQUEST_TIMEOUT_SECONDS = 5.0

# Bounded retry on transport failure. The ceiling is small so a caller that
# treats a request as fail-fast exits promptly once it is reached.
_MAX_ATTEMPTS = 5
_RETRY_BASE_DELAY_SECONDS = 0.5


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
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                reply = await self._send(cmd)
            except Exception as exc:
                logger.warning(
                    "Worker API request '%s' attempt %d/%d failed: %s", cmd.kind, attempt, _MAX_ATTEMPTS, exc
                )
                if attempt == _MAX_ATTEMPTS:
                    raise
                await asyncio.sleep(_RETRY_BASE_DELAY_SECONDS * attempt)
                continue
            return msgspec.msgpack.decode(reply, type=GrctlAPIResponse)

        raise AssertionError("unreachable")  # loop either returns or raises

    async def _send(self, cmd: Command) -> bytes:
        subject = manifest.worker_command_subject()
        data = command_encoder(cmd, enc_hook=self._codec.enc_hook)
        msg = await self._nc.request(subject, data, timeout=_REQUEST_TIMEOUT_SECONDS)
        return msg.data
