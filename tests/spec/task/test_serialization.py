import asyncio
import multiprocessing
import os
from datetime import timedelta
from typing import Any

import msgspec
import pytest
import ulid
from pydantic import BaseModel

from grctl.models import HistoryKind
from grctl.nats.connection import Connection
from grctl.worker import Context, task
from grctl.worker.worker import Worker
from grctl.workflow import Directive, Workflow
from tests.spec.task.helpers import wait_for_task_history

_WORKER_INIT_DELAY = 0.5
_WORKFLOW_TIMEOUT = timedelta(seconds=120)
_REPLAY_WORKER_ACK_WAIT_SECONDS = "0.5"


class PydanticPayload(BaseModel):
    name: str
    count: int
    tags: list[str]


class StructPayload(msgspec.Struct):
    name: str
    count: int
    tags: list[str]


def _unique_wf_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


def _terminate(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=5.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=1.0)


def _configure_fast_replay_redelivery() -> None:
    os.environ.setdefault("ENGINE_NATS_WORKER_ACK_WAIT", _REPLAY_WORKER_ACK_WAIT_SECONDS)


def _struct_output_replay_worker(wf_type: str, pause_event=None) -> None:
    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")
        wf = Workflow(workflow_type=wf_type)

        @task
        async def build_payload() -> StructPayload:
            return StructPayload(name="replayed", count=7, tags=["a", "b"])

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.step(work_step)

        @wf.step()
        async def work_step(ctx: Context) -> Directive:
            result = await build_payload()
            assert isinstance(result, StructPayload)
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete(msgspec.to_builtins(result))

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[wf], connection=conn)
        await wk.start()

    asyncio.run(run())


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
async def test_task_accepts_primitive_inputs(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_primitive_input"))

    @task
    async def inspect_payload(value: Any) -> Any:
        assert value == payload
        assert type(value) is type(payload)
        return value

    @wf.start()
    async def start(ctx: Context, value: Any) -> Directive:
        result = await inspect_payload(value)
        return ctx.next.complete(result)

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
async def test_task_accepts_dict_and_list_inputs(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_container_input"))

    @task
    async def inspect_payload(value: Any) -> Any:
        assert value == payload
        assert type(value) is type(payload)
        return value

    @wf.start()
    async def start(ctx: Context, value: Any) -> Directive:
        result = await inspect_payload(value)
        return ctx.next.complete(result)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={"value": payload},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == payload

    task_events = await wait_for_task_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [HistoryKind.task_started, HistoryKind.task_completed],
    )

    assert task_events[0].msg.args == {"value": payload}  # ty:ignore[unresolved-attribute]


async def test_task_accepts_pydantic_input(worker, grctl_client) -> None:
    payload = {"name": "pydantic-input", "count": 11, "tags": ["one", "two"]}
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_pydantic_input"))

    @task
    async def inspect_payload(value: PydanticPayload) -> dict[str, Any]:
        assert isinstance(value, PydanticPayload)
        return value.model_dump(mode="python")

    @wf.start()
    async def start(ctx: Context, value: dict[str, Any]) -> Directive:
        result = await inspect_payload(PydanticPayload.model_validate(value))
        return ctx.next.complete(result)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={"value": payload},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == payload

    task_events = await wait_for_task_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [HistoryKind.task_started, HistoryKind.task_completed],
    )

    assert task_events[0].msg.args == {"value": payload}  # ty:ignore[unresolved-attribute]


async def test_task_accepts_msgspec_struct_input(worker, grctl_client) -> None:
    payload = {"name": "struct-input", "count": 13, "tags": ["one", "two"]}
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_struct_input"))

    @task
    async def inspect_payload(value: StructPayload) -> dict[str, Any]:
        assert isinstance(value, StructPayload)
        return msgspec.to_builtins(value)

    @wf.start()
    async def start(ctx: Context, value: dict[str, Any]) -> Directive:
        result = await inspect_payload(StructPayload(**value))
        return ctx.next.complete(result)

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={"value": payload},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == payload

    task_events = await wait_for_task_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [HistoryKind.task_started, HistoryKind.task_completed],
    )

    assert task_events[0].msg.args == {"value": payload}  # ty:ignore[unresolved-attribute]


@pytest.mark.parametrize(
    "payload",
    [
        "hello",
        42,
        3.5,
        True,
    ],
)
async def test_task_returns_primitive_output(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_primitive_output"))

    @task
    async def build_payload() -> Any:
        return payload

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await build_payload()
        assert result == payload
        assert type(result) is type(payload)
        return ctx.next.complete(result)

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
        {"name": "dict-output", "count": 17, "tags": ["a", "b"], "nested": {"enabled": True}},
        ["list-output", 19, {"enabled": True}],
    ],
)
async def test_task_returns_dict_and_list_output(worker, grctl_client, payload: Any) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_container_output"))

    @task
    async def build_payload() -> Any:
        return payload

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await build_payload()
        assert result == payload
        assert type(result) is type(payload)
        return ctx.next.complete(result)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == payload


async def test_task_returns_pydantic_output(worker, grctl_client) -> None:
    payload = {"name": "pydantic-output", "count": 23, "tags": ["a", "b"]}
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_pydantic_output"))

    @task
    async def build_payload() -> PydanticPayload:
        return PydanticPayload.model_validate(payload)

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await build_payload()
        assert isinstance(result, PydanticPayload)
        return ctx.next.complete(result.model_dump(mode="python"))

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == payload


async def test_task_returns_msgspec_struct_output(worker, grctl_client) -> None:
    payload = {"name": "struct-output", "count": 29, "tags": ["a", "b"]}
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_struct_output"))

    @task
    async def build_payload() -> StructPayload:
        return StructPayload(**payload)  # ty:ignore[invalid-argument-type]

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await build_payload()
        assert isinstance(result, StructPayload)
        return ctx.next.complete(msgspec.to_builtins(result))

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == payload


async def test_task_returns_none_output(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_task_serialization_none_output"))

    @task
    async def build_payload() -> None:
        return None

    @wf.start()
    async def start(ctx: Context) -> Directive:
        result = await build_payload()
        assert result is None
        return ctx.next.complete(result)

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result is None


async def test_task_struct_output_is_preserved_on_replay(grctl_client) -> None:
    pause_event = multiprocessing.Event()
    wf_type = _unique_wf_type("spec_task_serialization_struct_replay")
    expected = {"name": "replayed", "count": 7, "tags": ["a", "b"]}

    worker_a = multiprocessing.Process(target=_struct_output_replay_worker, args=(wf_type, pause_event), daemon=True)
    worker_b = multiprocessing.Process(target=_struct_output_replay_worker, args=(wf_type,), daemon=True)
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf_type,
        id=wf_id,
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    try:
        await wait_for_task_history(
            grctl_client,
            wf_id,
            handle.run_info.id,
            [HistoryKind.task_started, HistoryKind.task_completed],
        )
        _terminate(worker_a)
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=60.0)

        assert result == expected

        task_events = await wait_for_task_history(
            grctl_client,
            wf_id,
            handle.run_info.id,
            [HistoryKind.task_started, HistoryKind.task_completed],
        )
        assert task_events[1].msg.output == {"result": expected}  # ty:ignore[unresolved-attribute]
    finally:
        _terminate(worker_a)
        _terminate(worker_b)
