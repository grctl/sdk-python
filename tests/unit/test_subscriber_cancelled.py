"""Tests for Subscriber ACK on CancelledError."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grctl.models import Directive, DirectiveKind, RunInfo, Start
from grctl.nats.subscriber import Subscriber


def _make_msg() -> AsyncMock:
    msg = AsyncMock()
    msg.data = b""
    msg.subject = "test.subject"
    msg.metadata = MagicMock()
    msg.metadata.num_delivered = 1
    return msg


def _make_directive() -> Directive:
    return Directive(
        id="dir-1",
        timestamp=datetime.now(UTC),
        kind=DirectiveKind.start,
        run_info=RunInfo(
            id="run-1",
            wf_id="wf-1",
            wf_type="TestWorkflow",
            created_at=datetime.now(UTC),
        ),
        msg=Start(input=None),
        attempt=0,
    )


@pytest.mark.asyncio
async def test_cancelled_error_acks_message() -> None:
    js = AsyncMock()
    manifest = MagicMock()
    run_manager = AsyncMock()
    subscriber = Subscriber(js=js, manifest=manifest, wf_types=["TestWorkflow"], run_manager=run_manager)

    msg = _make_msg()
    directive = _make_directive()

    task = asyncio.create_task(asyncio.sleep(10))
    task.cancel()

    run_manager.handle_next_directive = AsyncMock(return_value=task)

    with patch("grctl.nats.subscriber.directive_decoder", return_value=directive):
        await subscriber._handle_message(msg)

    msg.ack.assert_awaited_once()
