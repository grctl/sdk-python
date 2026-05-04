import asyncio
from datetime import UTC, datetime

import pytest
import ulid

from grctl.models import (
    CmdKind,
    Command,
    Directive,
    DirectiveKind,
    HistoryEvent,
    HistoryKind,
    RunInfo,
    RunStarted,
    Start,
    StartCmd,
    command_decoder,
    directive_decoder,
    history_decoder,
)
from grctl.nats.manifest import NatsManifest
from grctl.nats.nats_client import get_nats_client
from grctl.nats.publisher import Publisher
from grctl.settings import get_settings


class MessageHandler:
    def __init__(self, ready: asyncio.Event, messages: list, nc, decoder, reply: bytes | None = None) -> None:
        self._ready = ready
        self._messages = messages
        self._nc = nc
        self._decoder = decoder
        self._reply = reply

    async def handle(self, msg) -> None:
        self._messages.append(self._decoder(msg.data))
        if self._reply is not None and msg.reply:
            await self._nc.publish(msg.reply, self._reply)
        self._ready.set()


@pytest.mark.asyncio
async def test_publisher_publish_history_sends_event() -> None:
    settings = get_settings()
    nc = await get_nats_client(settings.nats_servers)
    js = nc.jetstream()

    manifest = NatsManifest.load()
    publisher = Publisher(nc=nc, js=js, manifest=manifest)

    wf_id = str(ulid.ULID())
    run_id = str(ulid.ULID())
    run_info = RunInfo(id=run_id, wf_id=wf_id, wf_type="TestWorkflow")

    event = HistoryEvent(
        wf_id=wf_id,
        run_id=run_id,
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.run_started,
        msg=RunStarted(),
    )

    ready = asyncio.Event()
    events: list[HistoryEvent] = []
    subject = manifest.history_subject(wf_id=wf_id, run_id=run_id)
    handler = MessageHandler(ready, events, nc, history_decoder)
    subscription = await nc.subscribe(subject, cb=handler.handle)

    await publisher.publish_history(run_info, event)
    await asyncio.wait_for(ready.wait(), timeout=5)

    assert len(events) == 1
    assert events[0].wf_id == wf_id
    assert events[0].run_id == run_id
    assert events[0].kind == HistoryKind.run_started

    await subscription.unsubscribe()
    await nc.drain()


@pytest.mark.asyncio
async def test_publisher_publish_next_directive_sends_directive() -> None:
    settings = get_settings()
    nc = await get_nats_client(settings.nats_servers)
    js = nc.jetstream()

    manifest = NatsManifest.load()
    publisher = Publisher(nc=nc, js=js, manifest=manifest)

    wf_id = str(ulid.ULID())
    run_id = str(ulid.ULID())
    run_info = RunInfo(id=run_id, wf_id=wf_id, wf_type="TestWorkflow")

    directive = Directive(
        id=str(ulid.ULID()),
        kind=DirectiveKind.start,
        run_info=run_info,
        timestamp=datetime.now(UTC),
        msg=Start(input=None),
    )

    ready = asyncio.Event()
    directives: list[Directive] = []
    subject = manifest.directive_subject(wf_type=run_info.wf_type, wf_id=wf_id, run_id=run_id)
    handler = MessageHandler(ready, directives, nc, directive_decoder)
    subscription = await nc.subscribe(subject, cb=handler.handle)

    await publisher.publish_next_directive(run_info, directive)
    await asyncio.wait_for(ready.wait(), timeout=5)

    assert len(directives) == 1
    assert directives[0].id == directive.id
    assert directives[0].kind == DirectiveKind.start
    assert directives[0].run_info.id == run_id
    assert directives[0].run_info.wf_id == wf_id

    await subscription.unsubscribe()
    await nc.drain()


@pytest.mark.asyncio
async def test_publisher_publish_cmd_requests_api_subject() -> None:
    settings = get_settings()
    nc = await get_nats_client(settings.nats_servers)
    js = nc.jetstream()

    manifest = NatsManifest.load()
    publisher = Publisher(nc=nc, js=js, manifest=manifest)

    wf_id = str(ulid.ULID())
    run_id = str(ulid.ULID())
    run_info = RunInfo(id=run_id, wf_id=wf_id, wf_type="TestWorkflow")

    cmd = Command(
        id=str(ulid.ULID()),
        kind=CmdKind.run_start,
        timestamp=datetime.now(UTC),
        msg=StartCmd(run_info=run_info, input=None),
    )

    ready = asyncio.Event()
    commands: list[Command] = []
    subject = manifest.api_subject(wf_id=wf_id)
    handler = MessageHandler(ready, commands, nc, command_decoder, reply=b"ok")
    subscription = await nc.subscribe(subject, cb=handler.handle)

    await publisher.publish_cmd(run_info, cmd)
    await asyncio.wait_for(ready.wait(), timeout=5)

    assert len(commands) == 1
    assert commands[0].kind == CmdKind.run_start
    assert isinstance(commands[0].msg, StartCmd)
    assert commands[0].msg.run_info.id == run_id
    assert commands[0].msg.run_info.wf_id == wf_id

    await subscription.unsubscribe()
    await nc.drain()
