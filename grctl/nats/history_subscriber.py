from collections.abc import Callable
from typing import TYPE_CHECKING

from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.js.api import DeliverPolicy

from grctl.logging_config import get_logger
from grctl.models import HistoryEvent, RunInfo, history_decoder
from grctl.nats.manifest import manifest

if TYPE_CHECKING:
    from nats.aio.subscription import Subscription

logger = get_logger(__name__)


class HistorySubscriber:
    """Delivers one run's history events to a handler, for as long as someone is watching.

    The subscription is ephemeral and outside any queue group, so every subscriber on a
    run receives that run's full event stream. A durable name or a queue group here would
    instead split one run's history between two watchers, each seeing half of it.

    DeliverPolicy.LAST is what makes join order irrelevant: a subscriber gets the newest
    event on the run's subject and everything after, and a run's outcome is always its
    newest event. So a watcher that arrives after the run finished still sees it finish.
    Earlier non-terminal events are not redelivered — anything that comes to depend on
    seeing those needs DeliverPolicy.ALL and a way to ignore what it has already seen.
    """

    def __init__(
        self,
        nc: NATSClient,
        wf_id: str,
        run_id: str,
        handler: Callable[[HistoryEvent], None],
    ) -> None:
        self._nc = nc
        self._js = nc.jetstream()
        self._history_subject = manifest.history_subject(wf_id=wf_id, run_id=run_id)
        self._history_stream = manifest.history_stream_name()
        self._handler = handler
        self._subscription: Subscription | None = None
        # Whether this listener has ever run, as distinct from whether it is running now:
        # stopping is terminal, so the two answers differ and only one of them gates start().
        self._started = False

    async def start(self) -> None:
        # Loud rather than tolerated: a second start means a caller lost track of which
        # listener is live, and the quiet outcomes are a duplicated event stream, or a
        # subscription revived after its owner settled with nothing left to close it.
        if self._started:
            raise RuntimeError(f"History listener for {self._history_subject} was already started")
        self._started = True

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


class NatsHistoryListenerFactory:
    """Builds a HistorySubscriber for a run, satisfying HistoryListenerFactory."""

    def __init__(self, nc: NATSClient) -> None:
        self.nc = nc

    def create(self, run_info: RunInfo, handler: Callable[[HistoryEvent], None]) -> HistorySubscriber:
        return HistorySubscriber(
            nc=self.nc,
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            handler=handler,
        )
