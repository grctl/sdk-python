from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.client import JetStreamContext
from nats.js.errors import FetchTimeoutError

from grctl.models import HistoryEvent, history_decoder
from grctl.nats.manifest import NatsManifest

_FETCH_BATCH_SIZE = 256
_FETCH_TIMEOUT_SECONDS = 0.25


async def fetch_step_history(
    js: JetStreamContext,
    manifest: NatsManifest,
    wf_id: str,
    run_id: str,
    history_seq_id: int,
) -> list[HistoryEvent]:
    if history_seq_id <= 0:
        return []

    history_subject = manifest.history_subject(wf_id=wf_id, run_id=run_id)
    history_stream = manifest.history_stream_name()
    subscription = await js.pull_subscribe(
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
        try:
            while True:
                messages = await subscription.fetch(batch=_FETCH_BATCH_SIZE, timeout=_FETCH_TIMEOUT_SECONDS)
                events.extend(history_decoder(msg.data) for msg in messages if msg.data)
        except (TimeoutError, FetchTimeoutError):
            return [event for event in events if event.operation_id]
    finally:
        await subscription.unsubscribe()
