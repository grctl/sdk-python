from datetime import timedelta
from typing import Any

import msgspec
import pytest
import ulid
from pydantic import BaseModel

from grctl.client import Client
from grctl.serde import serializer
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.workflows import unique_workflow_type


class PydanticPayload(BaseModel):
    name: str
    count: int
    tags: list[str]


class StructPayload(msgspec.Struct):
    name: str
    count: int
    tags: list[str]


class RegisteredPayload:
    def __init__(self, value: str) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RegisteredPayload) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)


@serializer(RegisteredPayload)
class RegisteredPayloadSerializer:
    def encode(self, value: RegisteredPayload) -> Any:
        return {"value": value.value}

    def decode(self, raw: Any) -> RegisteredPayload:
        return RegisteredPayload(raw["value"])


@pytest.mark.parametrize(
    "payload",
    [
        "hello",
        42,
        3.5,
        True,
        None,
    ],
)
async def test_workflow_accepts_primitive_inputs(worker, grctl_client: Client, payload: Any) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_primitive_input"))

    @wf.step(start=True)
    async def start(ctx: Context, value: Any) -> Directive:
        assert value == payload
        assert type(value) is type(payload)
        return ctx.next.complete(value)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": payload},
        timeout=timedelta(seconds=30),
    )

    assert result == payload


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "dict-case", "count": 3, "tags": ["a", "b"], "nested": {"enabled": True}},
        ["list-case", 3, {"enabled": True}],
    ],
)
async def test_workflow_accepts_dict_and_list_inputs(worker, grctl_client: Client, payload: Any) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_container_input"))

    @wf.step(start=True)
    async def start(ctx: Context, value: Any) -> Directive:
        assert value == payload
        assert type(value) is type(payload)
        return ctx.next.complete(value)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": payload},
        timeout=timedelta(seconds=30),
    )

    assert result == payload


async def test_workflow_roundtrips_msgspec_struct(worker, grctl_client: Client) -> None:
    struct = StructPayload(name="struct-input", count=13, tags=["one", "two"])
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_struct_input"))

    @wf.step(start=True)
    async def start(ctx: Context, value: StructPayload) -> Directive:
        assert isinstance(value, StructPayload)
        assert value == struct
        return ctx.next.complete(value)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": struct},
        timeout=timedelta(seconds=30),
        return_type=StructPayload,
    )

    assert result == struct


async def test_workflow_roundtrips_pydantic(worker, grctl_client: Client) -> None:
    model = PydanticPayload(name="pydantic-input", count=11, tags=["one", "two"])
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_pydantic_input"))

    @wf.step(start=True)
    async def start(ctx: Context, value: PydanticPayload) -> Directive:
        assert isinstance(value, PydanticPayload)
        assert value == model
        return ctx.next.complete(value)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": model},
        timeout=timedelta(seconds=30),
        return_type=PydanticPayload,
    )

    assert result == model


async def test_workflow_untyped_result_is_pydantic_primitives(worker, grctl_client: Client) -> None:
    """An untyped result is a plain primitive value."""
    model = PydanticPayload(name="pydantic-untyped-result", count=23, tags=["x", "y"])
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_pydantic_untyped"))

    @wf.step(start=True)
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(model)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type, id=str(ulid.ULID()), input={}, timeout=timedelta(seconds=30)
    )

    assert result == model.model_dump()


async def test_workflow_untyped_result_is_custom_primitives(worker, grctl_client: Client) -> None:
    """An untyped custom result is its serializer's primitive output."""
    value = RegisteredPayload("custom-untyped-result")
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_custom_untyped"))

    @wf.step(start=True)
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(value)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type, id=str(ulid.ULID()), input={}, timeout=timedelta(seconds=30)
    )

    assert result == {"value": value.value}


@pytest.mark.parametrize(
    "payload",
    [
        "hello",
        42,
        3.5,
        True,
    ],
)
async def test_workflow_returns_primitive_output(worker, grctl_client: Client, payload: Any) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_primitive_output"))

    @wf.step(start=True)
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(payload)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == payload
    assert type(result) is type(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "dict-output", "count": 17, "tags": ["a", "b"], "nested": {"enabled": True}},
        ["list-output", 19, {"enabled": True}],
    ],
)
async def test_workflow_returns_dict_and_list_output(worker, grctl_client: Client, payload: Any) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_container_output"))

    @wf.step(start=True)
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(payload)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == payload


async def test_start_workflow_result_deserializes_pydantic_output(worker, grctl_client: Client) -> None:
    model = PydanticPayload(name="pydantic-handle", count=31, tags=["x", "y"])
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_handle_pydantic"))

    @wf.step(start=True)
    async def start(ctx: Context, value: PydanticPayload) -> Directive:
        return ctx.next.complete(value)

    await worker([wf])

    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": model},
        timeout=timedelta(seconds=30),
        return_type=PydanticPayload,
    )
    result = await handle.result(timeout=30)

    assert isinstance(result, PydanticPayload)
    assert result == model


async def test_start_workflow_result_deserializes_struct_output(worker, grctl_client: Client) -> None:
    struct = StructPayload(name="struct-handle", count=37, tags=["x", "y"])
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_handle_struct"))

    @wf.step(start=True)
    async def start(ctx: Context, value: StructPayload) -> Directive:
        return ctx.next.complete(value)

    await worker([wf])

    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": struct},
        timeout=timedelta(seconds=30),
        return_type=StructPayload,
    )
    result = await handle.result(timeout=30)

    assert isinstance(result, StructPayload)
    assert result == struct


async def test_workflow_returns_none_output(worker, grctl_client: Client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_wf_serialization_none_output"))

    @wf.step(start=True)
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(None)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result is None
