import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Protocol

import msgspec
from nats.client.message import Message
from nats.client.subscription import Subscription

from grctl.logging_config import get_logger
from grctl.models import Command
from grctl.models.api import GrctlAPIResponse
from grctl.models.command import command_decoder
from grctl.nats.manifest import manifest

logger = get_logger(__name__)


class CoreCommandClient(Protocol):
    """Core NATS operations needed to receive and answer worker commands."""

    async def publish(self, subject: str | bytes, payload: bytes) -> None: ...

    async def subscribe(
        self,
        subject: str | bytes,
        *,
        max_pending_messages: int,
        max_pending_bytes: int,
    ) -> Subscription: ...


class WorkerCmdSubscriber:
    """Owns the lifecycle of the grctl_worker_cmd.{worker_id} core NATS subscription.

    Decodes the wire envelope and forwards the Command to `handler`, relaying
    its success/failure back as the reply. Dispatch on CmdKind is business
    logic this subscriber doesn't own.
    """

    def __init__(
        self,
        nc: CoreCommandClient,
        worker_id: str,
        handler: Callable[[Command], Awaitable[bool]],
    ) -> None:
        self._nc = nc
        self._worker_id = worker_id
        self._handler = handler
        self._subscription: Subscription | None = None
        self._consumer_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        subject = manifest.worker_cmd_subject(self._worker_id)
        self._subscription = await self._nc.subscribe(
            subject,
            max_pending_messages=1_000,
            max_pending_bytes=8 * 1024 * 1024,
        )
        self._consumer_task = asyncio.create_task(
            self._consume(self._subscription),
            name=f"worker-command-{self._worker_id}",
        )
        logger.debug("Subscribed to worker command channel: %s", subject)

    async def stop(self) -> None:
        if self._subscription is not None:
            await self._subscription.unsubscribe()
            self._subscription = None
        if self._consumer_task is not None:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None

    async def _consume(self, subscription: Subscription) -> None:
        async for msg in subscription:
            await self._on_message(msg)

    async def _on_message(self, msg: Message) -> None:
        try:
            cmd = command_decoder(msg.data)
        except Exception:
            logger.exception("Failed to decode worker command subject=%s", msg.subject)
            success = False
        else:
            try:
                success = await self._handler(cmd)
            except Exception:
                logger.exception("Failed to handle worker command subject=%s", msg.subject)
                success = False

        if msg.reply:
            await self._nc.publish(msg.reply, msgspec.msgpack.encode(GrctlAPIResponse(success=success)))
