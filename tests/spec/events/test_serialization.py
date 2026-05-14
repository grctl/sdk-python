import asyncio
from datetime import timedelta
from typing import Any

import msgspec
import pytest
import ulid
from pydantic import BaseModel

from grctl.client import Client
from grctl.models import HistoryKind
from grctl.models.history import EventReceived
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess
from tests.spec.workflows import unique_workflow_type


class StructPayload(msgspec.Struct):
    name: str
    count: int
    tags: list[str]


class PydanticPayload(BaseModel):
    name: str
    count: int
    tags: list[str]


@pytest.mark.parametrize(
    ("payload", "send_payload"),
    [
        pytest.param("hello-string", {"value": "hello-string"}, id="test_event_payload_string_value_preserved"),
        pytest.param(42, {"value": 42}, id="test_event_payload_integer_value_preserved"),
        pytest.param(True, {"value": True}, id="test_event_payload_boolean_value_preserved"),
        pytest.param(
            {"nested": {"key": "val"}, "count": 3, "flag": True},
            {"value": {"nested": {"key": "val"}, "count": 3, "flag": True}},
            id="test_event_payload_nested_dict_preserved",
        ),
        pytest.param([1, "two", True], {"value": [1, "two", True]}, id="test_event_payload_list_value_preserved"),
        pytest.param(None, None, id="test_event_payload_none_is_allowed"),
    ],
)
async def test_event_payload_preserved(worker, grctl_client: Client, payload: Any, send_payload: Any) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_event_serial"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait_for_event()

    @wf.event()
    async def receive(ctx: Context, value: Any = None) -> Directive:
        return ctx.next.complete(value)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    # try:
    history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
    await history.wait_for_kind(HistoryKind.wait_event_started)
    await handle.send("receive", send_payload)

    result = await asyncio.wait_for(handle.future, timeout=30)
    assert result == payload
    if payload is not None:
        assert type(result) is type(payload)

    event, _ = await history.wait_for_kind(HistoryKind.event_received)
    assert isinstance(event.msg, EventReceived)
    assert event.msg.event_name == "receive"
    assert event.msg.payload == send_payload
    # finally:
    # await handle.future.stop()


async def test_event_payload_msgspec_struct_preserved(worker, grctl_client: Client) -> None:
    struct = StructPayload(name="struct-event", count=7, tags=["a", "b"])
    wf = Workflow(workflow_type=unique_workflow_type("spec_event_serial_struct"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait_for_event()

    @wf.event()
    async def receive(ctx: Context, value: StructPayload) -> Directive:
        return ctx.next.complete(value)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        await history.wait_for_kind(HistoryKind.wait_event_started)
        await handle.send("receive", {"value": struct})

        result = await asyncio.wait_for(handle.future, timeout=30)
        assert msgspec.convert(result, StructPayload) == struct
    finally:
        await handle.future.stop()


async def test_event_payload_pydantic_model_preserved(worker, grctl_client: Client) -> None:
    model = PydanticPayload(name="pydantic-event", count=11, tags=["x", "y"])
    wf = Workflow(workflow_type=unique_workflow_type("spec_event_serial_pydantic"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait_for_event()

    @wf.event()
    async def receive(ctx: Context, value: PydanticPayload) -> Directive:
        return ctx.next.complete(value)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        await history.wait_for_kind(HistoryKind.wait_event_started)
        await handle.send("receive", {"value": model})

        result = await asyncio.wait_for(handle.future, timeout=30)
        assert PydanticPayload.model_validate(result) == model
    finally:
        await handle.future.stop()
