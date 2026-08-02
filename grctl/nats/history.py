import logging

from nats.aio.client import Client as NATSClient
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import FetchTimeoutError, ServiceUnavailableError

from grctl.models import (
    HistoryEvent,
    history_decoder,
    history_encoder,
)
from grctl.nats.codec import CodecRegistry
from grctl.nats.manifest import NatsManifest

logger = logging.getLogger(__name__)

_FETCH_BATCH_SIZE = 256
_FETCH_TIMEOUT_SECONDS = 0.25


class NatsHistory:
    """Single NATS impl of the history reader and writer ports.

    Reader side (get_run_history / fetch_step_history) pull-subscribes over the
    run's history subject; writer side (append) publishes onto it. The domain
    depends only on the narrow reader/writer protocols this satisfies.
    """

    def __init__(self, nc: NATSClient, manifest: NatsManifest, codec: CodecRegistry) -> None:
        self.js = nc.jetstream()
        self.manifest = manifest
        self.codec = codec

    async def get_run_history(self, wf_id: str, run_id: str) -> list[HistoryEvent]:
        history_subject = self.manifest.history_subject(wf_id=wf_id, run_id=run_id)
        history_stream = self.manifest.history_stream_name()
        subscription = await self.js.pull_subscribe(
            subject=history_subject,
            stream=history_stream,
            config=ConsumerConfig(
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.NONE,
                inactive_threshold=1.0,
            ),
        )

        events: list[HistoryEvent] = []
        try:
            while True:
                messages = await subscription.fetch(batch=_FETCH_BATCH_SIZE, timeout=_FETCH_TIMEOUT_SECONDS)
                events.extend(history_decoder(msg.data) for msg in messages if msg.data)
        except (TimeoutError, FetchTimeoutError, ServiceUnavailableError):
            pass
        finally:
            await subscription.unsubscribe()
        return events

    async def fetch_step_history(
        self,
        wf_id: str,
        run_id: str,
        history_seq_id: int,
    ) -> list[HistoryEvent]:
        if history_seq_id <= 0:
            return []

        history_subject = self.manifest.history_subject(wf_id=wf_id, run_id=run_id)
        history_stream = self.manifest.history_stream_name()
        subscription = await self.js.pull_subscribe(
            subject=history_subject,
            stream=history_stream,
            config=ConsumerConfig(
                deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=history_seq_id,
                ack_policy=AckPolicy.NONE,
                inactive_threshold=1.0,
            ),
        )

        events: list[HistoryEvent] = []
        try:
            while True:
                messages = await subscription.fetch(batch=_FETCH_BATCH_SIZE, timeout=_FETCH_TIMEOUT_SECONDS)
                events.extend(history_decoder(msg.data) for msg in messages if msg.data)
        except (TimeoutError, FetchTimeoutError):
            pass
        finally:
            await subscription.unsubscribe()
        return [event for event in events if event.operation_id]

    async def append(self, event: HistoryEvent) -> None:
        subject = self.manifest.history_subject(wf_id=event.wf_id, run_id=event.run_id)
        data = history_encoder(event, enc_hook=self.codec.enc_hook)
        await self.js.publish(subject, data)
