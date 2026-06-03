"""Tests for WorkerCmdSubscriber."""

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import msgspec
import pytest

from grctl.models.api import GrctlAPIResponse
from grctl.models.command import (
    CmdKind,
    Command,
    WorkerTerminateRunCmd,
    command_encoder,
)
from grctl.worker.worker_cmd_subscriber import WorkerCmdSubscriber


def _make_subscriber(run_manager=None) -> WorkerCmdSubscriber:
    nc = AsyncMock()
    manifest = MagicMock()
    manifest.worker_cmd_subject.return_value = "grctl_worker_cmd.worker-1"
    if run_manager is None:
        run_manager = MagicMock()
    return WorkerCmdSubscriber(nc=nc, manifest=manifest, worker_id="worker-1", run_manager=run_manager)


def _make_msg(data: bytes) -> AsyncMock:
    msg = AsyncMock()
    msg.data = data
    msg.subject = "grctl_worker_cmd.worker-1"
    return msg


def _terminate_run_cmd(run_id: str) -> bytes:
    cmd = Command(
        id="cmd-1",
        kind=CmdKind.worker_terminate_run,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        msg=WorkerTerminateRunCmd(run_id=run_id),
        sender_id="s_server-001",
    )
    return command_encoder(cmd)


@pytest.mark.asyncio
async def test_start_subscribes_to_worker_cmd_subject() -> None:
    nc = AsyncMock()
    manifest = MagicMock()
    manifest.worker_cmd_subject.return_value = "grctl_worker_cmd.worker-1"
    sub = WorkerCmdSubscriber(nc=nc, manifest=manifest, worker_id="worker-1", run_manager=MagicMock())
    await sub.start()
    nc.subscribe.assert_awaited_once_with(
        "grctl_worker_cmd.worker-1", cb=sub._on_message
    )


@pytest.mark.asyncio
async def test_stop_unsubscribes() -> None:
    sub = _make_subscriber()
    mock_sub = AsyncMock()
    sub._subscription = mock_sub
    await sub.stop()
    mock_sub.unsubscribe.assert_awaited_once()
    assert sub._subscription is None


@pytest.mark.asyncio
async def test_terminate_run_dispatches_to_run_manager_and_replies_success() -> None:
    run_manager = MagicMock()
    run_manager.terminate_run.return_value = True
    sub = _make_subscriber(run_manager=run_manager)

    msg = _make_msg(_terminate_run_cmd("run-42"))
    await sub._on_message(msg)

    run_manager.terminate_run.assert_called_once_with("run-42")
    replied = msg.respond.call_args[0][0]
    assert replied == msgspec.msgpack.encode(GrctlAPIResponse(success=True))


@pytest.mark.asyncio
async def test_terminate_run_not_found_replies_false() -> None:
    run_manager = MagicMock()
    run_manager.terminate_run.return_value = False
    sub = _make_subscriber(run_manager=run_manager)

    msg = _make_msg(_terminate_run_cmd("no-such-run"))
    await sub._on_message(msg)

    replied = msg.respond.call_args[0][0]
    assert replied == msgspec.msgpack.encode(GrctlAPIResponse(success=False))


@pytest.mark.asyncio
async def test_unknown_kind_logs_warning_and_replies_false(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sub = _make_subscriber()

    # Build a command with a known kind but no registered handler path
    # We patch _dispatch_command to simulate an unknown kind falling through
    cmd = Command(
        id="cmd-2",
        kind=CmdKind.run_cancel,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        msg=WorkerTerminateRunCmd(run_id="run-1"),
        sender_id="s_server-001",
    )

    with caplog.at_level(logging.WARNING):
        result = await sub._dispatch_command(cmd)

    assert result is False
    assert any("unknown" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_corrupt_payload_logs_exception_and_replies_false(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sub = _make_subscriber()
    msg = _make_msg(b"not valid msgpack \xff\xfe")

    with caplog.at_level(logging.ERROR):
        await sub._on_message(msg)

    replied = msg.respond.call_args[0][0]
    assert replied == msgspec.msgpack.encode(GrctlAPIResponse(success=False))
