from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest

from grctl.models import (
    Command,
    RegisterCmd,
    WorkflowTypeDef,
    command_decoder,
    command_encoder,
)
from grctl.models.command import CmdKind
from grctl.models.directive import Directive
from grctl.nats.connection import Connection
from grctl.worker.errors import RegistrationError
from grctl.worker.registration import (
    REGISTRATION_MAX_ATTEMPTS,
    _to_type_def,
    build_catalog,
    register_workflow_types,
)
from grctl.workflow.workflow import Workflow


def _order_workflow() -> Workflow:
    wf = Workflow(workflow_type="order_wf")

    @wf.start()
    async def start(ctx, order_id: str) -> Directive:
        raise NotImplementedError

    @wf.step()
    async def reserve(ctx, item: str) -> Directive:
        raise NotImplementedError

    @wf.step()
    async def charge(ctx, amount: int) -> Directive:
        raise NotImplementedError

    @wf.event(name="approve")
    async def on_approve(ctx) -> Directive:
        raise NotImplementedError

    @wf.query(name="status")
    async def status(ctx) -> Directive:
        raise NotImplementedError

    return wf


def test_build_catalog_matches_decorators() -> None:
    catalog = build_catalog([_order_workflow()])

    assert len(catalog) == 1
    type_def = catalog[0]
    assert type_def.type == "order_wf"
    assert type_def.start_step == "start"
    assert sorted(type_def.steps) == ["charge", "reserve"]
    assert type_def.events == ["approve"]
    assert type_def.queries == ["status"]


def test_build_catalog_without_start_handler_yields_empty_start_step() -> None:
    wf = Workflow(workflow_type="no_start_wf")
    catalog = build_catalog([wf])
    assert catalog[0].start_step == ""


def test_register_cmd_command_roundtrip() -> None:
    cmd = Command(
        id="01J000000000000000000000CMD",
        kind=CmdKind.worker_register,
        timestamp=datetime.now(UTC),
        msg=RegisterCmd(
            worker_id="w_abc@host",
            types=[
                WorkflowTypeDef(
                    type="order_wf",
                    start_step="start",
                    steps=["reserve", "charge"],
                    events=["approve"],
                    queries=["status"],
                )
            ],
        ),
        sender_id="w_abc@host",
    )

    decoded = command_decoder(command_encoder(cmd))

    assert decoded.kind == CmdKind.worker_register
    assert isinstance(decoded.msg, RegisterCmd)
    assert decoded.msg == cmd.msg


def test_register_cmd_wire_keys_match_go_tags() -> None:
    """The inner message must encode with the exact field names Go's msgpack tags expect."""
    msg = RegisterCmd(
        worker_id="w_abc@host",
        types=[
            WorkflowTypeDef(
                type="order_wf",
                start_step="start",
                steps=["reserve"],
                events=["approve"],
                queries=["status"],
            )
        ],
    )

    as_dict = msgspec.msgpack.decode(msgspec.msgpack.encode(msg))

    assert set(as_dict.keys()) == {"worker_id", "types"}
    assert set(as_dict["types"][0].keys()) == {"type", "start_step", "steps", "events", "queries", "start_step_timeout_ms"}


def _connection_with_reply(reply_data: bytes) -> AsyncMock:
    connection = AsyncMock(spec=Connection)
    connection.manifest = MagicMock()
    connection.manifest.worker_command_subject.return_value = "grctl_api.worker"
    reply = MagicMock()
    reply.data = reply_data
    connection.nc = AsyncMock()
    connection.nc.request = AsyncMock(return_value=reply)
    return connection


def _ack_bytes() -> bytes:
    """Encode a success ACK the way the Go server does (omitempty payload/error)."""
    return msgspec.msgpack.encode({"success": True})


@pytest.mark.asyncio
async def test_register_workflow_types_succeeds_on_ack() -> None:
    connection = _connection_with_reply(_ack_bytes())

    await register_workflow_types(connection, "w_abc@host", build_catalog([_order_workflow()]))

    connection.nc.request.assert_awaited_once()


@pytest.mark.asyncio
async def test_register_workflow_types_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("grctl.worker.registration.asyncio.sleep", AsyncMock())

    reply = MagicMock()
    reply.data = _ack_bytes()
    connection = AsyncMock(spec=Connection)
    connection.manifest = MagicMock()
    connection.manifest.worker_command_subject.return_value = "grctl_api.worker"
    connection.nc = AsyncMock()
    connection.nc.request = AsyncMock(side_effect=[TimeoutError("timeout"), reply])

    await register_workflow_types(connection, "w_abc@host", build_catalog([_order_workflow()]))

    assert connection.nc.request.await_count == 2


@pytest.mark.asyncio
async def test_register_workflow_types_raises_after_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("grctl.worker.registration.asyncio.sleep", AsyncMock())

    connection = AsyncMock(spec=Connection)
    connection.manifest = MagicMock()
    connection.manifest.worker_command_subject.return_value = "grctl_api.worker"
    connection.nc = AsyncMock()
    connection.nc.request = AsyncMock(side_effect=TimeoutError("timeout"))

    with pytest.raises(RegistrationError):
        await register_workflow_types(connection, "w_abc@host", build_catalog([_order_workflow()]))

    assert connection.nc.request.await_count == REGISTRATION_MAX_ATTEMPTS


def test_to_type_def_with_start_timeout() -> None:
    wf = Workflow(workflow_type="timed_wf")

    @wf.start(timeout=timedelta(seconds=30))
    async def start(ctx, x: str) -> Directive:
        raise NotImplementedError

    type_def = _to_type_def(wf)

    assert type_def.start_step_timeout_ms == 30_000


def test_to_type_def_no_start_timeout() -> None:
    wf = Workflow(workflow_type="default_wf")

    @wf.start()
    async def start(ctx, x: str) -> Directive:
        raise NotImplementedError

    type_def = _to_type_def(wf)

    assert type_def.start_step_timeout_ms == 0
