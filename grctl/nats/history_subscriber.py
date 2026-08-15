import asyncio
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from nats.jetstream import JetStream
from nats.jetstream.message import Message
from ulid import ULID

from grctl.logging_config import get_logger
from grctl.models import HistoryEvent, RunInfo, history_decoder
from grctl.nats.manifest import manifest
from grctl.settings import get_settings

if TYPE_CHECKING:
    from nats.jetstream.consumer.pull import PullConsumer

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
        jetstream: JetStream,
        wf_id: str,
        run_id: str,
        handler: Callable[[HistoryEvent], None],
    ) -> None:
        self._jetstream = jetstream
        self._history_subject = manifest.history_subject(wf_id=wf_id, run_id=run_id)
        self._history_stream = manifest.history_stream_name()
        self._handler = handler
        self._consumer: PullConsumer | None = None
        self._consume_task: asyncio.Task[None] | None = None
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

        self._consumer = cast(
            "PullConsumer",
            await self._jetstream.create_consumer(
                self._history_stream,
                name=f"history-listener-{ULID()}",
                filter_subject=self._history_subject,
                deliver_policy="last",
                ack_policy="explicit",
                inactive_threshold=timedelta(seconds=30),
            ),
        )
        self._consume_task = asyncio.create_task(self._consume())
        logger.debug("Subscribed to history subject %s", self._history_subject)

    async def stop(self) -> None:
        consume_task, self._consume_task = self._consume_task, None
        consumer, self._consumer = self._consumer, None

        if consume_task is not None:
            consume_task.cancel()
            await asyncio.gather(consume_task, return_exceptions=True)

        if consumer is not None:
            await self._jetstream.delete_consumer(self._history_stream, consumer.name)
            logger.debug("Unsubscribed from history subject %s", self._history_subject)

    async def _consume(self) -> None:
        consumer = self._consumer
        if consumer is None:
            raise RuntimeError("History listener started without a consumer")
        while True:
            try:
                batch = await consumer.fetch(max_messages=1)
                async for msg in batch:
                    await self._on_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "History subscription fetch failed subject=%s; retrying",
                    self._history_subject,
                )
                await asyncio.sleep(get_settings().nats_reconnect_time_wait)

    async def _on_message(self, msg: Message) -> None:
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

    def __init__(self, jetstream: JetStream) -> None:
        self._jetstream = jetstream

    def create(self, run_info: RunInfo, handler: Callable[[HistoryEvent], None]) -> HistorySubscriber:
        return HistorySubscriber(
            jetstream=self._jetstream,
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            handler=handler,
        )
