from collections.abc import Callable
from typing import TYPE_CHECKING

from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.js.api import DeliverPolicy

from grctl.logging_config import get_logger
from grctl.models import HistoryEvent, history_decoder
from grctl.nats.manifest import NatsManifest

if TYPE_CHECKING:
    from nats.aio.subscription import Subscription

logger = get_logger(__name__)


class HistorySubscriber:
    """Manages JetStream subscription for workflow history events."""

    def __init__(
        self,
        nc: NATSClient,
        wf_id: str,
        run_id: str,
        handler: Callable[[HistoryEvent], None],
    ) -> None:
        self._nc = nc
        self._js = nc.jetstream()
        self._manifest = NatsManifest.load()
        self._history_subject = self._manifest.history_subject(wf_id=wf_id, run_id=run_id)
        self._history_stream = self._manifest.history_stream_name()
        self._handler = handler
        self._subscription: Subscription | None = None

    async def start(self) -> None:
        if self._subscription is not None:
            msg = "HistorySubscriber already started"
            raise RuntimeError(msg)

        self._subscription = await self._js.subscribe(
            self._history_subject,
            stream=self._history_stream,
            cb=self._on_message,
            manual_ack=True,
            deliver_policy=DeliverPolicy.LAST,
        )
        logger.debug("Subscribed to history subject %s", self._history_subject)

    async def stop(self) -> None:
        if self._subscription is not None:
            await self._subscription.unsubscribe()
            self._subscription = None
            logger.debug("Unsubscribed from history subject %s", self._history_subject)

    async def _on_message(self, msg: Msg) -> None:
        try:
            event: HistoryEvent = history_decoder(msg.data)
            self._handler(event)
        except Exception:
            logger.exception("Error handling history event")
        finally:
            try:
                await msg.ack()
            except Exception:
                logger.exception("Error acking history event")
