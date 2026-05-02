import asyncio
from datetime import UTC, datetime

import pytest
import ulid

from grctl.models import HistoryEvent, HistoryKind, RunStarted, history_encoder
from grctl.nats.history_sub import HistorySubscriber
from grctl.nats.manifest import NatsManifest
from grctl.nats.nats_client import get_nats_client
from grctl.settings import get_settings


class HistoryEventHandler:
    def __init__(self, ready: asyncio.Event, events: list[HistoryEvent]) -> None:
        self._ready = ready
        self._events = events

    def __call__(self, event: HistoryEvent) -> None:
        self._events.append(event)
        self._ready.set()


@pytest.mark.asyncio
async def test_history_subscriber_receives_event() -> None:
    settings = get_settings()
    nc = await get_nats_client(settings.nats_servers)
    js = nc.jetstream()

    wf_id = str(ulid.ULID())
    run_id = str(ulid.ULID())

    ready = asyncio.Event()
    events: list[HistoryEvent] = []
    handler = HistoryEventHandler(ready, events)

    subscriber = HistorySubscriber(nc=nc, wf_id=wf_id, run_id=run_id, handler=handler)
    await subscriber.start()

    manifest = NatsManifest.load()
    subject = manifest.history_subject(wf_id=wf_id, run_id=run_id)
    event = HistoryEvent(
        wf_id=wf_id,
        run_id=run_id,
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.run_started,
        msg=RunStarted(),
    )

    await js.publish(subject, history_encoder(event))
    await asyncio.wait_for(ready.wait(), timeout=5)

    assert len(events) == 1
    assert events[0].wf_id == wf_id
    assert events[0].run_id == run_id
    assert events[0].kind == HistoryKind.run_started
    assert isinstance(events[0].msg, RunStarted)

    await subscriber.stop()
    assert subscriber._subscription is None
    await nc.drain()
