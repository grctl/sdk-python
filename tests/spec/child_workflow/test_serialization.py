from datetime import timedelta
from typing import Any

import msgspec
import pytest
import ulid

from grctl.client import Client
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.workflows import unique_workflow_type

_WORKFLOW_TIMEOUT = timedelta(seconds=60)


class StructPayload(msgspec.Struct):
    name: str
    count: int
    tags: list[str]


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
async def test_child_accepts_primitive_inputs(worker, grctl_client: Client, payload: Any) -> None:
    parent_wf_type = unique_workflow_type("spec_child_ser_primitive_input_parent")
    child_wf_type = unique_workflow_type("spec_child_ser_primitive_input_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context, value: Any) -> Directive:
        assert value == payload
        assert type(value) is type(payload)
        return ctx.next.complete("child-done")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child", workflow_input={"value": payload})
        await handle.future
        return ctx.next.complete("ok")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "ok"


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "dict-case", "count": 3, "tags": ["a", "b"]},
        ["list-case", 3, True],
    ],
)
async def test_child_accepts_dict_and_list_inputs(worker, grctl_client: Client, payload: Any) -> None:
    parent_wf_type = unique_workflow_type("spec_child_ser_container_input_parent")
    child_wf_type = unique_workflow_type("spec_child_ser_container_input_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context, value: Any) -> Directive:
        assert value == payload
        assert type(value) is type(payload)
        return ctx.next.complete("child-done")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child", workflow_input={"value": payload})
        await handle.future
        return ctx.next.complete("ok")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "ok"


async def test_child_accepts_struct_input(worker, grctl_client: Client) -> None:
    struct = StructPayload(name="struct-input", count=7, tags=["x", "y"])
    parent_wf_type = unique_workflow_type("spec_child_ser_struct_input_parent")
    child_wf_type = unique_workflow_type("spec_child_ser_struct_input_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context, value: StructPayload) -> Directive:
        assert isinstance(value, StructPayload)
        assert value == struct
        return ctx.next.complete("child-done")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child", workflow_input={"value": struct})
        await handle.future
        return ctx.next.complete("ok")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "ok"


@pytest.mark.parametrize(
    "payload",
    [
        "hello",
        42,
        3.5,
        True,
    ],
)
async def test_child_future_returns_primitive_output(worker, grctl_client: Client, payload: Any) -> None:
    parent_wf_type = unique_workflow_type("spec_child_ser_primitive_output_parent")
    child_wf_type = unique_workflow_type("spec_child_ser_primitive_output_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.complete(payload)

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
        result = await handle.future
        assert result == payload
        assert type(result) is type(payload)
        return ctx.next.complete("ok")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "ok"


async def test_child_future_returns_struct_output(worker, grctl_client: Client) -> None:
    expected = StructPayload(name="struct-output", count=13, tags=["a", "b"])
    parent_wf_type = unique_workflow_type("spec_child_ser_struct_output_parent")
    child_wf_type = unique_workflow_type("spec_child_ser_struct_output_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.complete(expected)

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
        raw = await handle.future
        # handle.future returns raw primitive; struct reconstruction is the caller's responsibility
        assert raw == {"name": "struct-output", "count": 13, "tags": ["a", "b"]}
        return ctx.next.complete("ok")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "ok"


@pytest.mark.parametrize(
    "payload",
    [
        "hello",
        42,
        3.5,
        True,
    ],
)
async def test_send_to_parent_preserves_primitive_payload(worker, grctl_client: Client, payload: Any) -> None:
    expected_payload = payload
    parent_wf_type = unique_workflow_type("spec_child_ser_send_primitive_parent")
    child_wf_type = unique_workflow_type("spec_child_ser_send_primitive_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        await ctx.send_to_parent("result", payload=expected_payload)
        return ctx.next.complete("child-done")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
        return ctx.next.wait()

    @parent_wf.event(name="result")
    async def on_result(ctx: Context, payload: Any) -> Directive:
        assert payload == expected_payload
        assert type(payload) is type(expected_payload)
        return ctx.next.complete("ok")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "ok"


async def test_send_to_parent_preserves_struct_payload(worker, grctl_client: Client) -> None:
    struct = StructPayload(name="event-payload", count=5, tags=["p", "q"])
    parent_wf_type = unique_workflow_type("spec_child_ser_send_struct_parent")
    child_wf_type = unique_workflow_type("spec_child_ser_send_struct_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        await ctx.send_to_parent("result", payload=struct)
        return ctx.next.complete("child-done")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
        return ctx.next.wait()

    @parent_wf.event(name="result")
    async def on_result(ctx: Context, payload: StructPayload) -> Directive:
        assert isinstance(payload, StructPayload)
        assert payload == struct
        return ctx.next.complete("ok")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "ok"
