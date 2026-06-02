"""Tests for Command wire format: sender_id round-trip."""

from datetime import UTC, datetime

import pytest

from grctl.models.command import (
    CancelCmd,
    CmdKind,
    Command,
    command_decoder,
    command_encoder,
)


def _make_cmd(sender_id: str = "c_abc123@myhost") -> Command:
    return Command(
        id="01J000000000000000000001",
        kind=CmdKind.run_cancel,
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        msg=CancelCmd(wf_id="wf-1", reason=None),
        sender_id=sender_id,
    )


def test_round_trip_preserves_sender_id() -> None:
    cmd = _make_cmd("c_abc123@myhost")
    decoded = command_decoder(command_encoder(cmd))
    assert decoded.sender_id == "c_abc123@myhost"


def test_round_trip_preserves_all_fields() -> None:
    cmd = _make_cmd("w_a1b2c.f7@myhost")
    decoded = command_decoder(command_encoder(cmd))
    assert decoded.id == cmd.id
    assert decoded.kind == cmd.kind
    assert decoded.sender_id == cmd.sender_id


def test_encode_empty_sender_id_raises() -> None:
    cmd = _make_cmd("")
    with pytest.raises(ValueError, match="sender"):
        command_encoder(cmd)
