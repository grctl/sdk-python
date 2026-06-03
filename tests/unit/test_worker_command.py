"""Tests for worker command dispatch."""

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from grctl.models.command import CancelCmd, CmdKind, Command
from grctl.worker.worker_cmd_subscriber import WorkerCmdSubscriber


def _make_subscriber() -> WorkerCmdSubscriber:
    return WorkerCmdSubscriber(
        nc=AsyncMock(),
        manifest=MagicMock(),
        worker_id="w_test.01@host",
        run_manager=MagicMock(),
    )


def _make_cmd(kind: CmdKind) -> Command:
    return Command(
        id="01J000000000000000000001",
        kind=kind,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        msg=CancelCmd(wf_id="wf-1", reason=None),
        sender_id="s_server-001",
    )


@pytest.mark.asyncio
async def test_dispatch_unknown_kind_logs_warning_and_does_not_raise(caplog: pytest.LogCaptureFixture) -> None:
    subscriber = _make_subscriber()
    cmd = _make_cmd(CmdKind.run_cancel)  # no handler registered for this kind

    with caplog.at_level(logging.WARNING, logger="grctl.worker.worker_cmd_subscriber"):
        result = await subscriber._dispatch_command(cmd)

    assert result is False
    assert any("unknown" in record.message.lower() for record in caplog.records)
