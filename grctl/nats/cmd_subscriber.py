from collections.abc import Awaitable, Callable

import msgspec
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from grctl.logging_config import get_logger
from grctl.models import Command
from grctl.models.api import GrctlAPIResponse
from grctl.models.command import command_decoder
from grctl.nats.manifest import NatsManifest

logger = get_logger(__name__)


class WorkerCmdSubscriber:
    """Owns the lifecycle of the grctl_worker_cmd.{worker_id} core NATS subscription.

    Decodes the wire envelope and forwards the Command to `handler`, relaying
    its success/failure back as the reply. Dispatch on CmdKind is business
    logic this subscriber doesn't own.
    """

    def __init__(
        self,
        nc: NatsClient,
        manifest: NatsManifest,
        worker_id: str,
        handler: Callable[[Command], Awaitable[bool]],
    ) -> None:
        self._nc = nc
        self._manifest = manifest
        self._worker_id = worker_id
        self._handler = handler
        self._subscription = None

    async def start(self) -> None:
        subject = self._manifest.worker_cmd_subject(self._worker_id)
        self._subscription = await self._nc.subscribe(subject, cb=self._on_message)
        logger.debug("Subscribed to worker command channel: %s", subject)

    async def stop(self) -> None:
        if self._subscription is not None:
            await self._subscription.unsubscribe()
            self._subscription = None

    async def _on_message(self, msg: Msg) -> None:
        try:
            cmd = command_decoder(msg.data)
        except Exception:
            logger.exception("Failed to decode worker command subject=%s", msg.subject)
            await msg.respond(msgspec.msgpack.encode(GrctlAPIResponse(success=False)))
            return

        success = await self._handler(cmd)
        await msg.respond(msgspec.msgpack.encode(GrctlAPIResponse(success=success)))
