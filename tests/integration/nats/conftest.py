"""Integration test fixtures for grctl.nats.

These tests exercise the adapters in `grctl/nats` against a real broker, because
what they encode — ACK/NAK redelivery, ack_wait expiry, queue-group distribution
— is broker behaviour that a mock cannot reproduce.

Assumes grctld is already running. Start it before running these tests:

    cd grctl && mise run start
"""

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, datetime

import pytest
import ulid
from nats.client import connect
from nats.jetstream import new as new_jetstream

from grctl.models import Directive, DirectiveKind, RunInfo, Start, directive_encoder
from grctl.nats.manifest import manifest
from grctl.nats.nats_client import get_nats_client
from grctl.nats.wf_subscriber import DirectiveHandler, Subscriber
from grctl.settings import get_settings

NATS_URL = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

ACK_WAIT_SECONDS = 2.0
"""Short enough that an un-ACKed message redelivers inside a test, long enough
that a healthy ACK path is never mistaken for one."""

PROGRESS_ACK_INTERVAL_SECONDS = 0.5

REDELIVERY_GRACE_SECONDS = ACK_WAIT_SECONDS + 1.5
"""Wait longer than ack_wait before asserting "delivered once". A shorter wait
would pass even if the subscriber never ACKed at all."""


@pytest.fixture
def unique_wf_type() -> str:
    """Return a workflow type no other test shares.

    Consumer name, queue group and filter subject are all derived from wf_type,
    so this is what keeps concurrent and repeated runs from seeing each other's
    messages on a long-lived server.
    """
    return f"WF{ulid.ULID()}"


@pytest.fixture
async def nc() -> AsyncIterator:
    """nats-core client used to drive core NATS request/reply."""
    client = await get_nats_client([NATS_URL])
    yield client
    with contextlib.suppress(Exception):
        await client.drain()


@pytest.fixture
async def jetstream() -> AsyncIterator:
    """nats-jetstream handle — the API `Subscriber` consumes through."""
    client = await connect(NATS_URL)
    yield new_jetstream(client)
    with contextlib.suppress(Exception):
        await client.close()


@pytest.fixture
def fast_ack_settings() -> Iterator[None]:
    """Shrink ack_wait so redelivery timing is testable in seconds, not minutes."""
    previous = {
        "ENGINE_NATS_WORKER_ACK_WAIT": os.environ.get("ENGINE_NATS_WORKER_ACK_WAIT"),
        "ENGINE_PROGRESS_ACK_INTERVAL_SECONDS": os.environ.get("ENGINE_PROGRESS_ACK_INTERVAL_SECONDS"),
    }
    os.environ["ENGINE_NATS_WORKER_ACK_WAIT"] = str(ACK_WAIT_SECONDS)
    os.environ["ENGINE_PROGRESS_ACK_INTERVAL_SECONDS"] = str(PROGRESS_ACK_INTERVAL_SECONDS)
    get_settings.cache_clear()

    yield

    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    get_settings.cache_clear()


def make_directive(wf_type: str) -> Directive:
    return Directive(
        id=str(ulid.ULID()),
        kind=DirectiveKind.start,
        run_info=RunInfo(id=str(ulid.ULID()), wf_id=str(ulid.ULID()), wf_type=wf_type),
        timestamp=datetime.now(UTC),
        msg=Start(input=None),
    )


def worker_task_subject(directive: Directive) -> str:
    """Subject the server publishes worker tasks to.

    Built here rather than read from the manifest because the SDK only ever
    consumes this subject — the publish side belongs to grctld, and these tests
    stand in for it.
    """
    run_info = directive.run_info
    return f"grctl_worker_task.{run_info.wf_type}.{run_info.wf_id}.{run_info.id}"


@pytest.fixture
def publish_directive(jetstream) -> Callable[[Directive], Awaitable[None]]:
    async def publish(directive: Directive) -> None:
        await jetstream.publish(worker_task_subject(directive), directive_encoder(directive))

    return publish


@pytest.fixture
def publish_raw(jetstream) -> Callable[[str, bytes], Awaitable[None]]:
    """Publish arbitrary bytes to a worker task subject, for decode-failure paths."""

    async def publish(wf_type: str, data: bytes) -> None:
        subject = f"grctl_worker_task.{wf_type}.{ulid.ULID()}.{ulid.ULID()}"
        await jetstream.publish(subject, data)

    return publish


@pytest.fixture
async def delete_consumer(jetstream) -> AsyncIterator[Callable[[str], None]]:
    """Track wf_types whose durable consumers should be removed after the test.

    The consumer is durable and named after wf_type, so without this every test
    leaves one behind on a long-lived server.
    """
    wf_types: list[str] = []

    yield wf_types.append

    for wf_type in wf_types:
        with contextlib.suppress(Exception):
            stream = await jetstream.get_stream(manifest.state_stream_name())
            await stream.delete_consumer(manifest.worker_task_queue_group(wf_type))


@pytest.fixture
def subscriber_logger() -> logging.Logger:
    return logging.getLogger("tests.integration.nats.subscriber")


@pytest.fixture
async def start_subscriber(jetstream, subscriber_logger, delete_consumer) -> AsyncIterator[Callable]:
    """Start a Subscriber on one wf_type and shut it down after the test."""
    started: list[Subscriber] = []

    async def start(wf_type: str, handler: DirectiveHandler) -> Subscriber:
        subscriber = Subscriber(
            js=jetstream,
            wf_types=[wf_type],
            directive_handler=handler,
            logger=subscriber_logger,
        )
        await subscriber.start()
        started.append(subscriber)
        delete_consumer(wf_type)
        return subscriber

    yield start

    for subscriber in started:
        # Bounded: stop() drains tasks_in_progress, and a test may leave one parked.
        with contextlib.suppress(Exception, asyncio.TimeoutError):
            await asyncio.wait_for(subscriber.stop(), timeout=5.0)
