"""Integration tests for Subscriber ACK semantics against a real NATS server.

These tests require the server to be running at localhost:4225 with the
grctl_state stream and grctl_worker_task_cons consumer already created.
"""

import asyncio
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock

import pytest
import ulid

from grctl.models import Directive, DirectiveKind, RunInfo, Start, directive_encoder
from grctl.nats.manifest import NatsManifest
from grctl.nats.nats_client import get_nats_client
from grctl.nats.subscriber import Subscriber
from grctl.settings import get_settings
from grctl.worker.run_manager import RunManager


def _load_manifest() -> NatsManifest:
    return NatsManifest.load()


def _make_directive(wf_type: str) -> Directive:
    return Directive(
        id=str(ulid.ULID()),
        kind=DirectiveKind.start,
        run_info=RunInfo(id=str(ulid.ULID()), wf_id=str(ulid.ULID()), wf_type=wf_type),
        timestamp=datetime.now(UTC),
        msg=Start(input=None),
    )


def _publish_subject(wf_type: str, directive: Directive) -> str:
    return f"grctl_worker_task.{wf_type}.{directive.run_info.wf_id}.{directive.run_info.id}"


async def _noop() -> None:
    pass


async def _failing() -> None:
    raise RuntimeError("step failed")


@pytest.mark.asyncio
async def test_ack_prevents_redelivery() -> None:
    """A successfully handled message is ACKed and not redelivered to the same worker."""
    settings = get_settings()
    nc = await get_nats_client(settings.nats_servers)
    js = nc.jetstream()
    manifest = _load_manifest()

    # Unique wf_type per test so messages don't cross between runs
    wf_type = f"WF{ulid.ULID()}"
    directive = _make_directive(wf_type)
    await js.publish(_publish_subject(wf_type, directive), directive_encoder(directive))

    delivery_count = 0
    first_delivery = asyncio.Event()
    run_manager = AsyncMock(spec=RunManager)

    async def on_directive(d: Directive) -> asyncio.Task:
        nonlocal delivery_count
        delivery_count += 1
        first_delivery.set()
        return asyncio.create_task(_noop())

    run_manager.handle_next_directive.side_effect = on_directive

    subscriber = Subscriber(
        js=js,
        manifest=manifest,
        wf_types=[wf_type],
        run_manager=cast("RunManager", run_manager),
    )
    await subscriber.start()

    await asyncio.wait_for(first_delivery.wait(), timeout=5.0)
    await asyncio.sleep(0.5)  # allow time for any spurious redelivery

    assert delivery_count == 1

    await subscriber.stop()
    await nc.drain()


@pytest.mark.asyncio
async def test_nak_causes_redelivery() -> None:
    """A NAKed message (init failure) is redelivered to the worker."""
    settings = get_settings()
    nc = await get_nats_client(settings.nats_servers)
    js = nc.jetstream()
    manifest = _load_manifest()

    wf_type = f"WF{ulid.ULID()}"
    directive = _make_directive(wf_type)
    await js.publish(_publish_subject(wf_type, directive), directive_encoder(directive))

    delivery_count = 0
    second_delivery = asyncio.Event()
    run_manager = AsyncMock(spec=RunManager)

    async def on_directive_fail(d: Directive) -> asyncio.Task:
        nonlocal delivery_count
        delivery_count += 1
        if delivery_count >= 2:
            second_delivery.set()
        raise ValueError("No workflow registered")

    run_manager.handle_next_directive.side_effect = on_directive_fail

    subscriber = Subscriber(
        js=js,
        manifest=manifest,
        wf_types=[wf_type],
        run_manager=cast("RunManager", run_manager),
    )
    await subscriber.start()

    await asyncio.wait_for(second_delivery.wait(), timeout=10.0)

    assert delivery_count >= 2

    await subscriber.stop()
    await nc.drain()


@pytest.mark.asyncio
async def test_runner_exception_acks_no_redelivery() -> None:
    """A runner exception still ACKs the message — no redelivery after a known failure."""
    settings = get_settings()
    nc = await get_nats_client(settings.nats_servers)
    js = nc.jetstream()
    manifest = _load_manifest()

    wf_type = f"WF{ulid.ULID()}"
    directive = _make_directive(wf_type)
    await js.publish(_publish_subject(wf_type, directive), directive_encoder(directive))

    delivery_count = 0
    first_delivery = asyncio.Event()
    run_manager = AsyncMock(spec=RunManager)

    async def on_directive_with_failing_task(d: Directive) -> asyncio.Task:
        nonlocal delivery_count
        delivery_count += 1
        first_delivery.set()
        return asyncio.create_task(_failing())

    run_manager.handle_next_directive.side_effect = on_directive_with_failing_task

    subscriber = Subscriber(
        js=js,
        manifest=manifest,
        wf_types=[wf_type],
        run_manager=cast("RunManager", run_manager),
    )
    await subscriber.start()

    await asyncio.wait_for(first_delivery.wait(), timeout=5.0)
    await asyncio.sleep(0.5)  # allow time for any spurious redelivery

    assert delivery_count == 1

    await subscriber.stop()
    await nc.drain()


@pytest.mark.asyncio
async def test_queue_group_distributes_messages_across_workers() -> None:
    """Two subscribers on the same queue group each receive distinct messages with no duplicates."""
    settings = get_settings()
    nc1 = await get_nats_client(settings.nats_servers)
    nc2 = await get_nats_client(settings.nats_servers)
    js1 = nc1.jetstream()
    js2 = nc2.jetstream()
    manifest = _load_manifest()

    wf_type = f"WF{ulid.ULID()}"
    directive1 = _make_directive(wf_type)
    directive2 = _make_directive(wf_type)
    await js1.publish(_publish_subject(wf_type, directive1), directive_encoder(directive1))
    await js1.publish(_publish_subject(wf_type, directive2), directive_encoder(directive2))

    worker1_ids: list[str] = []
    worker2_ids: list[str] = []
    both_done = asyncio.Event()

    run_manager1 = AsyncMock(spec=RunManager)
    run_manager2 = AsyncMock(spec=RunManager)

    async def on_worker1(d: Directive) -> asyncio.Task:
        worker1_ids.append(d.id)
        if len(worker1_ids) + len(worker2_ids) >= 2:
            both_done.set()
        return asyncio.create_task(_noop())

    async def on_worker2(d: Directive) -> asyncio.Task:
        worker2_ids.append(d.id)
        if len(worker1_ids) + len(worker2_ids) >= 2:
            both_done.set()
        return asyncio.create_task(_noop())

    run_manager1.handle_next_directive.side_effect = on_worker1
    run_manager2.handle_next_directive.side_effect = on_worker2

    subscriber1 = Subscriber(
        js=js1,
        manifest=manifest,
        wf_types=[wf_type],
        run_manager=cast("RunManager", run_manager1),
    )
    subscriber2 = Subscriber(
        js=js2,
        manifest=manifest,
        wf_types=[wf_type],
        run_manager=cast("RunManager", run_manager2),
    )
    await subscriber1.start()
    await subscriber2.start()

    await asyncio.wait_for(both_done.wait(), timeout=10.0)

    all_handled = worker1_ids + worker2_ids
    assert len(all_handled) == 2
    assert len(set(all_handled)) == 2  # no duplicate directive IDs across workers

    await subscriber1.stop()
    await subscriber2.stop()
    await nc1.drain()
    await nc2.drain()
