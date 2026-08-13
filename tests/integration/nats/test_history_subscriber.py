"""History event subscription, against a real broker.

The client tails a run's history to surface progress. What matters is that the
subscription starts, survives a bad event, and genuinely stops on `stop()` —
a subscription that outlives its owner leaks callbacks into the next run.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest
import ulid

from grctl.models import HistoryEvent, HistoryKind, RunStarted, history_encoder
from grctl.nats.history_subscriber import HistorySubscriber
from grctl.nats.manifest import manifest

DELIVERY_TIMEOUT_SECONDS = 10.0
SETTLE_SECONDS = 1.0


@pytest.fixture
def run_ids() -> tuple[str, str]:
    """Return a wf_id/run_id pair no other test shares.

    The history subject is built from both, so this is what isolates one test's
    events from another's on a long-lived server.
    """
    return str(ulid.ULID()), str(ulid.ULID())


class EventRecorder:
    """Collects delivered events; optionally fails the first handler call."""

    def __init__(self, *, fail_first: bool = False) -> None:
        self.events: list[HistoryEvent] = []
        self.received = asyncio.Event()
        self._fail_first = fail_first
        self._calls = 0

    def __call__(self, event: HistoryEvent) -> None:
        self._calls += 1
        if self._fail_first and self._calls == 1:
            raise RuntimeError("handler blew up")
        self.events.append(event)
        self.received.set()


def make_event(wf_id: str, run_id: str) -> HistoryEvent:
    return HistoryEvent(
        wf_id=wf_id,
        run_id=run_id,
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.run_started,
        msg=RunStarted(),
    )


@pytest.fixture
def publish_event(nc) -> Callable[[HistoryEvent], Awaitable[None]]:
    js = nc.jetstream()

    async def publish(event: HistoryEvent) -> None:
        subject = manifest.history_subject(wf_id=event.wf_id, run_id=event.run_id)
        await js.publish(subject, history_encoder(event))

    return publish


async def test_published_event_reaches_the_handler(nc, run_ids, publish_event) -> None:
    wf_id, run_id = run_ids
    recorder = EventRecorder()
    subscriber = HistorySubscriber(nc=nc, wf_id=wf_id, run_id=run_id, handler=recorder)
    await subscriber.start()

    await publish_event(make_event(wf_id, run_id))
    await asyncio.wait_for(recorder.received.wait(), timeout=DELIVERY_TIMEOUT_SECONDS)

    assert len(recorder.events) == 1
    assert recorder.events[0].wf_id == wf_id
    assert recorder.events[0].run_id == run_id
    assert isinstance(recorder.events[0].msg, RunStarted)

    await subscriber.stop()


async def test_starting_twice_reuses_the_single_subscription(nc, run_ids, publish_event) -> None:
    """Start is idempotent, and a second start must not open a second subscription.

    A run's listener is started from more than one call site — a child handle is started
    by the step that creates it and attached again when a retry replays that step — so a
    double start has to be a no-op. Subscribing twice would deliver every event twice and
    leak the first subscription, with nothing left holding it to close it.
    """
    wf_id, run_id = run_ids
    recorder = EventRecorder()
    subscriber = HistorySubscriber(nc=nc, wf_id=wf_id, run_id=run_id, handler=recorder)
    await subscriber.start()
    await subscriber.start()

    await publish_event(make_event(wf_id, run_id))
    await asyncio.wait_for(recorder.received.wait(), timeout=DELIVERY_TIMEOUT_SECONDS)
    await asyncio.sleep(SETTLE_SECONDS)

    assert len(recorder.events) == 1

    # The single stop() the owner performs must leave nothing behind.
    await subscriber.stop()
    await publish_event(make_event(wf_id, run_id))
    await asyncio.sleep(SETTLE_SECONDS)

    assert len(recorder.events) == 1


async def test_handler_failure_does_not_break_the_subscription(nc, run_ids, publish_event) -> None:
    """One malformed or mishandled event must not deafen the client to the rest."""
    wf_id, run_id = run_ids
    recorder = EventRecorder(fail_first=True)
    subscriber = HistorySubscriber(nc=nc, wf_id=wf_id, run_id=run_id, handler=recorder)
    await subscriber.start()

    await publish_event(make_event(wf_id, run_id))
    await publish_event(make_event(wf_id, run_id))
    await asyncio.wait_for(recorder.received.wait(), timeout=DELIVERY_TIMEOUT_SECONDS)

    assert len(recorder.events) == 1

    await subscriber.stop()


async def test_stop_ends_delivery(nc, run_ids, publish_event) -> None:
    wf_id, run_id = run_ids
    recorder = EventRecorder()
    subscriber = HistorySubscriber(nc=nc, wf_id=wf_id, run_id=run_id, handler=recorder)
    await subscriber.start()
    await subscriber.stop()

    await publish_event(make_event(wf_id, run_id))
    await asyncio.sleep(SETTLE_SECONDS)

    assert recorder.events == []
