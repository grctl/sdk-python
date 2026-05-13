from datetime import timedelta
from typing import Any

import msgspec
import pytest
import ulid

from grctl.worker import Context
from grctl.workflow import Directive, Workflow


class StructPayload(msgspec.Struct):
    name: str
    count: int
    tags: list[str]


def _unique_wf_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


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
async def test_workflow_accepts_primitive_inputs(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_serialization_primitive_input"))

    @wf.start()
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
        {"name": "dict-input", "count": 3, "tags": ["a", "b"], "nested": {"enabled": True}},
        ["list-input", 5, {"enabled": True}],
    ],
)
async def test_workflow_accepts_dict_and_list_inputs(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_serialization_container_input"))

    @wf.start()
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


async def test_workflow_accepts_msgspec_struct_input(worker, grctl_client) -> None:
    payload = StructPayload(name="struct-input", count=7, tags=["one", "two"])
    expected = msgspec.to_builtins(payload)
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_serialization_struct_input"))

    @wf.start()
    async def start(ctx: Context, value: dict[str, Any]) -> Directive:
        reconstructed = StructPayload(**value)
        assert reconstructed == payload
        return ctx.next.complete(msgspec.to_builtins(reconstructed))

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": payload},
        timeout=timedelta(seconds=30),
    )

    assert result == expected


@pytest.mark.parametrize(
    "payload",
    [
        "hello",
        42,
        3.5,
        True,
    ],
)
async def test_workflow_returns_primitive_output(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_serialization_primitive_output"))

    @wf.start()
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


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "dict-output", "count": 11, "tags": ["a", "b"], "nested": {"enabled": True}},
        ["list-output", 13, {"enabled": True}],
    ],
)
async def test_workflow_returns_dict_and_list_output(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_serialization_container_output"))

    @wf.start()
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


async def test_workflow_returns_none_output(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_serialization_none_output"))

    @wf.start()
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


async def test_workflow_returns_msgspec_struct_output(worker, grctl_client) -> None:
    payload = StructPayload(name="struct-output", count=17, tags=["a", "b"])
    expected = msgspec.to_builtins(payload)
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_serialization_struct_output"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(msgspec.to_builtins(payload))

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == expected
