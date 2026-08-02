"""Worker command channel — core NATS request/reply, against a real broker.

The server blocks on the reply to these requests, so the guarantee under test is
that every request gets exactly one answer: on success, on rejection, and on a
payload the worker cannot read.
"""

from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime

import msgspec
import pytest
import ulid
from nats.errors import NoRespondersError

from grctl.models import CmdKind, Command, DescribeCmd, GrctlAPIResponse, command_encoder
from grctl.nats.cmd_subscriber import WorkerCmdSubscriber
from grctl.nats.manifest import manifest

REQUEST_TIMEOUT_SECONDS = 3.0


@pytest.fixture
def worker_id() -> str:
    return f"worker-{ulid.ULID()}"


class CommandRecorder:
    """Stands in for the worker's command dispatcher."""

    def __init__(self, *, accepts: bool = True) -> None:
        self.commands: list[Command] = []
        self._accepts = accepts

    async def __call__(self, command: Command) -> bool:
        self.commands.append(command)
        return self._accepts


def make_command() -> Command:
    return Command(
        id=str(ulid.ULID()),
        kind=CmdKind.run_describe,
        timestamp=datetime.now(UTC),
        msg=DescribeCmd(wf_id=str(ulid.ULID())),
        sender_id="server",
    )


@pytest.fixture
async def start_cmd_subscriber(nc, worker_id) -> AsyncIterator[Callable]:
    """Start a subscriber and wait for the server to register it.

    Without the flush a request can outrun its own subscription and come back as
    NoRespondersError, which would make these tests flaky rather than wrong.
    """
    started: list[WorkerCmdSubscriber] = []

    async def start(handler) -> WorkerCmdSubscriber:
        subscriber = WorkerCmdSubscriber(nc=nc, worker_id=worker_id, handler=handler)
        await subscriber.start()
        await nc.flush()
        started.append(subscriber)
        return subscriber

    yield start

    for subscriber in started:
        await subscriber.stop()


@pytest.fixture
def request_command(nc, worker_id) -> Callable:
    async def send(payload: bytes) -> GrctlAPIResponse:
        reply = await nc.request(
            manifest.worker_cmd_subject(worker_id),
            payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        return msgspec.msgpack.decode(reply.data, type=GrctlAPIResponse)

    return send


async def test_command_reaches_handler_and_is_acknowledged(start_cmd_subscriber, request_command) -> None:
    recorder = CommandRecorder(accepts=True)
    await start_cmd_subscriber(recorder)

    command = make_command()
    response = await request_command(command_encoder(command))

    assert response.success is True
    assert len(recorder.commands) == 1
    assert recorder.commands[0].id == command.id
    assert recorder.commands[0].kind == CmdKind.run_describe


async def test_handler_rejection_is_reported_to_caller(start_cmd_subscriber, request_command) -> None:
    """A worker that refuses a command must say so rather than let the server hang."""
    recorder = CommandRecorder(accepts=False)
    await start_cmd_subscriber(recorder)

    response = await request_command(command_encoder(make_command()))

    assert response.success is False
    assert len(recorder.commands) == 1


async def test_undecodable_command_is_rejected_without_reaching_handler(start_cmd_subscriber, request_command) -> None:
    recorder = CommandRecorder(accepts=True)
    await start_cmd_subscriber(recorder)

    response = await request_command(b"not a command")

    assert response.success is False
    assert recorder.commands == []


async def test_stop_unsubscribes_from_the_command_channel(nc, start_cmd_subscriber, request_command) -> None:
    recorder = CommandRecorder(accepts=True)
    subscriber = await start_cmd_subscriber(recorder)

    await subscriber.stop()
    await nc.flush()

    with pytest.raises(NoRespondersError):
        await request_command(command_encoder(make_command()))

    assert recorder.commands == []
