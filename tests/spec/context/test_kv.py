"""Spec tests: ctx.store KV cross-step persistence."""

import asyncio
from datetime import timedelta

import msgspec
import ulid
from pydantic import BaseModel

from grctl.worker import Context
from grctl.worker.store import StoreKeyNotFoundError
from grctl.workflow import Directive, Workflow
from tests.spec.workflows import unique_workflow_type

_WORKFLOW_TIMEOUT = timedelta(seconds=30)


class PydanticKVPayload(BaseModel):
    label: str
    count: int


class StructKVPayload(msgspec.Struct):
    label: str
    count: int


async def test_kv_value_persists_across_steps(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_ctx_kv_persist"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        ctx.store.put("key", "hello")
        return ctx.next.step(read_step)

    @wf.step()
    async def read_step(ctx: Context) -> Directive:
        value = await ctx.store.get("key")
        return ctx.next.complete(value)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf.workflow_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)
    result = await asyncio.wait_for(handle.future, timeout=30.0)

    assert result == "hello"


async def test_kv_multiple_types_persist(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_ctx_kv_types"))
    pydantic_val = PydanticKVPayload(label="hello", count=5)
    struct_val = StructKVPayload(label="world", count=7)

    @wf.start()
    async def start(ctx: Context) -> Directive:
        ctx.store.put("str_key", "text")
        ctx.store.put("int_key", 42)
        ctx.store.put("float_key", 3.14)
        ctx.store.put("bool_key", True)
        ctx.store.put("list_key", [1, 2, 3])
        ctx.store.put("dict_key", {"a": 1})
        ctx.store.put("pydantic_key", pydantic_val)
        ctx.store.put("struct_key", struct_val)
        return ctx.next.step(read_step)

    @wf.step()
    async def read_step(ctx: Context) -> Directive:
        str_val = await ctx.store.get("str_key")
        int_val = await ctx.store.get("int_key")
        float_val = await ctx.store.get("float_key")
        bool_val = await ctx.store.get("bool_key")
        list_val = await ctx.store.get("list_key")
        dict_val = await ctx.store.get("dict_key")
        pydantic_result = await ctx.store.get("pydantic_key", PydanticKVPayload)
        struct_result = await ctx.store.get("struct_key", StructKVPayload)
        assert isinstance(pydantic_result, PydanticKVPayload)
        assert isinstance(struct_result, StructKVPayload)
        return ctx.next.complete({
            "str": str_val,
            "int": int_val,
            "float": float_val,
            "bool": bool_val,
            "list": list_val,
            "dict": dict_val,
            "pydantic": pydantic_result.model_dump(),
            "struct": {"label": struct_result.label, "count": struct_result.count},
        })

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf.workflow_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)
    result = await asyncio.wait_for(handle.future, timeout=30.0)

    assert result["str"] == "text"
    assert result["int"] == 42
    assert abs(result["float"] - 3.14) < 1e-9
    assert result["bool"] is True
    assert result["list"] == [1, 2, 3]
    assert result["dict"] == {"a": 1}
    assert result["pydantic"] == {"label": "hello", "count": 5}
    assert result["struct"] == {"label": "world", "count": 7}


async def test_kv_get_raises_for_missing_key(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_ctx_kv_missing"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(read_step)

    @wf.step()
    async def read_step(ctx: Context) -> Directive:
        try:
            await ctx.store.get("never_put")
        except StoreKeyNotFoundError:
            return ctx.next.complete("raised")
        return ctx.next.complete("no_error")

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(type=wf.workflow_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT)
    result = await asyncio.wait_for(handle.future, timeout=30.0)

    assert result == "raised"
