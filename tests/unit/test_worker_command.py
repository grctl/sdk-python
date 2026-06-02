"""Tests for worker command dispatch."""

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from grctl.models.command import CancelCmd, CmdKind, Command
from grctl.nats.connection import Connection
from grctl.worker.worker import Worker
from grctl.workflow.workflow import Workflow


def _make_workflow(workflow_type: str) -> Workflow:
    wf = MagicMock(spec=Workflow)
    wf.workflow_type = workflow_type
    return wf


def _make_worker() -> Worker:
    connection = AsyncMock(spec=Connection)
    connection.js = AsyncMock()
    connection.manifest = MagicMock()
    connection.publisher = AsyncMock()
    wf = _make_workflow("wf_type_a")
    return Worker(workflows=[wf], connection=connection)


def _make_cmd(kind: CmdKind) -> Command:
    return Command(
        id="01J000000000000000000001",
        kind=kind,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        msg=CancelCmd(wf_id="wf-1", reason=None),
        sender_id="s_server-001",
    )


def test_dispatch_unknown_kind_logs_warning_and_does_not_raise(caplog: pytest.LogCaptureFixture) -> None:
    worker = _make_worker()
    cmd = _make_cmd(CmdKind.run_cancel)  # no handler registered for any kind yet

    with caplog.at_level(logging.WARNING, logger="grctl.worker.worker"):
        worker._dispatch_command(cmd)

    assert any("unknown" in record.message.lower() for record in caplog.records)
