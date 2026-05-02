import asyncio
import contextlib
import dataclasses
import multiprocessing
from collections.abc import AsyncIterator, Callable
from datetime import timedelta
from typing import Any

import msgspec
import pytest
import ulid
from pydantic import BaseModel

from grctl.client.client import Client
from grctl.models import Directive, HistoryKind, TaskStarted
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.task import task
from grctl.worker.worker import Worker
from grctl.workflow import Workflow
from tests.e2e.helpers import _terminate_process, _wait_for_history_event

_WORKER_INIT_DELAY = 0.5


@dataclasses.dataclass
class DataclassPayload:
    name: str
    count: int
    tags: list[str]


class PydanticPayload(BaseModel):
    name: str
    count: int
    tags: list[str]


class StructPayload(msgspec.Struct):
    name: str
    count: int
    tags: list[str]


def _workflow_type(case_name: str) -> str:
    return f"SerializationWorkflow_{case_name}"


def _primitive_type(case_name: str) -> type:
    return {
        "string": str,
        "int": int,
        "float": float,
        "bool": bool,
    }[case_name]


def _build_primitive_workflow(case_name: str) -> Workflow:
    workflow = Workflow(workflow_type=_workflow_type(case_name))
    primitive_type = _primitive_type(case_name)

    @task
    async def inspect_payload(payload: Any) -> Any:
        assert isinstance(payload, primitive_type), (
            f"task: expected {primitive_type.__name__}, got {type(payload).__name__}: {payload!r}"
        )
        return payload

    @workflow.start()
    async def start(ctx: Context, payload: Any) -> Directive:  # type: ignore[valid-type]
        assert isinstance(payload, primitive_type), (
            f"start: expected {primitive_type.__name__}, got {type(payload).__name__}: {payload!r}"
        )
        ctx.store.put("payload", payload)
        ctx.store.put("task_result", await inspect_payload(payload))
        return ctx.next.step(read_back)

    @workflow.step()
    async def read_back(ctx: Context) -> Directive:
        return ctx.next.complete(
            {
                "stored_payload": await ctx.store.get("payload"),
                "task_result": await ctx.store.get("task_result"),
            }
        )

    return workflow


def _build_dict_workflow() -> Workflow:
    workflow = Workflow(workflow_type=_workflow_type("dict"))

    @task
    async def inspect_payload(payload: dict[str, Any]) -> dict[str, Any]:
        assert isinstance(payload, dict), f"task: expected dict, got {type(payload).__name__}: {payload!r}"
        return payload

    @workflow.start()
    async def start(ctx: Context, payload: dict[str, Any]) -> Directive:
        assert isinstance(payload, dict), f"start: expected dict, got {type(payload).__name__}: {payload!r}"
        ctx.store.put("payload", payload)
        ctx.store.put("task_result", await inspect_payload(payload))
        return ctx.next.step(read_back)

    @workflow.step()
    async def read_back(ctx: Context) -> Directive:
        return ctx.next.complete(
            {
                "stored_payload": await ctx.store.get("payload"),
                "task_result": await ctx.store.get("task_result"),
            }
        )

    return workflow


def _build_dataclass_workflow() -> Workflow:
    workflow = Workflow(workflow_type=_workflow_type("dataclass"))

    @task
    async def inspect_payload(payload: DataclassPayload) -> dict[str, Any]:
        assert isinstance(payload, DataclassPayload), (
            f"task: expected DataclassPayload, got {type(payload).__name__}: {payload!r}"
        )
        return dataclasses.asdict(payload)

    @workflow.start()
    async def start(ctx: Context, payload: dict[str, Any]) -> Directive:
        dataclass_payload = DataclassPayload(**payload)
        ctx.store.put("payload", dataclass_payload)
        ctx.store.put("task_result", await inspect_payload(dataclass_payload))
        return ctx.next.step(read_back)

    @workflow.step()
    async def read_back(ctx: Context) -> Directive:
        stored_payload = await ctx.store.get("payload")
        task_result = await ctx.store.get("task_result")
        assert isinstance(stored_payload, dict), (
            f"read_back: stored_payload expected dict, got {type(stored_payload).__name__}: {stored_payload!r}"
        )
        assert isinstance(task_result, dict), (
            f"read_back: task_result expected dict, got {type(task_result).__name__}: {task_result!r}"
        )
        return ctx.next.complete(
            {
                "stored_payload": stored_payload,
                "task_result": task_result,
            }
        )

    return workflow


def _build_struct_workflow() -> Workflow:
    workflow = Workflow(workflow_type=_workflow_type("struct"))

    @task
    async def inspect_payload(payload: StructPayload) -> dict[str, Any]:
        assert isinstance(payload, StructPayload), (
            f"task: expected StructPayload, got {type(payload).__name__}: {payload!r}"
        )
        return msgspec.to_builtins(payload)

    @workflow.start()
    async def start(ctx: Context, payload: dict[str, Any]) -> Directive:
        struct_payload = StructPayload(**payload)
        ctx.store.put("payload", struct_payload)
        ctx.store.put("task_result", await inspect_payload(struct_payload))
        return ctx.next.step(read_back)

    @workflow.step()
    async def read_back(ctx: Context) -> Directive:
        stored_payload = await ctx.store.get("payload")
        task_result = await ctx.store.get("task_result")
        assert isinstance(stored_payload, dict), (
            f"read_back: stored_payload expected dict, got {type(stored_payload).__name__}: {stored_payload!r}"
        )
        assert isinstance(task_result, dict), (
            f"read_back: task_result expected dict, got {type(task_result).__name__}: {task_result!r}"
        )
        return ctx.next.complete(
            {
                "stored_payload": stored_payload,
                "task_result": task_result,
            }
        )

    return workflow


def _build_pydantic_typed_workflow() -> Workflow:
    workflow = Workflow(workflow_type=_workflow_type("pydantic_typed"))

    @workflow.start()
    async def start(ctx: Context, payload: PydanticPayload) -> Directive:
        assert isinstance(payload, PydanticPayload), (
            f"start: expected PydanticPayload, got {type(payload).__name__}: {payload!r}"
        )
        ctx.store.put("name", payload.name)
        return ctx.next.step(read_back)

    @workflow.step()
    async def read_back(ctx: Context) -> Directive:
        name = await ctx.store.get("name")
        return ctx.next.complete({"store_value": name})

    return workflow


def _build_pydantic_workflow() -> Workflow:
    workflow = Workflow(workflow_type=_workflow_type("pydantic"))

    @task
    async def inspect_payload(payload: PydanticPayload) -> dict[str, Any]:
        assert isinstance(payload, PydanticPayload), (
            f"task: expected PydanticPayload, got {type(payload).__name__}: {payload!r}"
        )
        return payload.model_dump(mode="python")

    @workflow.start()
    async def start(ctx: Context, payload: dict[str, Any]) -> Directive:
        model = PydanticPayload.model_validate(payload)
        result = await inspect_payload(model)
        return ctx.next.complete(result)

    return workflow


_WORKFLOW_BUILDERS: dict[str, Callable[[], Workflow]] = {
    "string": lambda: _build_primitive_workflow("string"),
    "int": lambda: _build_primitive_workflow("int"),
    "float": lambda: _build_primitive_workflow("float"),
    "bool": lambda: _build_primitive_workflow("bool"),
    "dict": _build_dict_workflow,
    "dataclass": _build_dataclass_workflow,
    "struct": _build_struct_workflow,
    "pydantic": _build_pydantic_workflow,
    "pydantic_typed": _build_pydantic_typed_workflow,
}


def _worker_process_main(case_name: str, timeout_seconds: float = 60.0) -> None:
    workflow = _WORKFLOW_BUILDERS[case_name]()

    async def run_worker() -> None:
        connection = await Connection.connect()
        worker = Worker(workflows=[workflow], connection=connection)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(worker.start(), timeout=timeout_seconds)

    asyncio.run(run_worker())


@contextlib.asynccontextmanager
async def _worker_running(case_name: str) -> AsyncIterator[tuple[Connection, Client]]:
    connection = await Connection.connect()
    client = Client(connection=connection)
    worker_process = multiprocessing.Process(
        target=_worker_process_main,
        args=(case_name,),
        daemon=True,
    )
    worker_process.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)
    try:
        yield connection, client
    finally:
        _terminate_process(worker_process)
        Connection.reset()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case_name", "workflow_input", "expected"),
    [
        ("string", {"payload": "hello"}, "hello"),
        ("int", {"payload": 42}, 42),
        ("float", {"payload": 3.5}, 3.5),
        ("bool", {"payload": True}, True),
    ],
)
async def test_serialization_primitives(case_name: str, workflow_input: Any, expected: Any) -> None:
    async with _worker_running(case_name) as (connection, client):
        workflow_id = str(ulid.ULID())
        handle = await client.start_workflow(
            type=_workflow_type(case_name),
            id=workflow_id,
            input=workflow_input,
            timeout=timedelta(seconds=30),
        )

        result = await asyncio.wait_for(handle.future, timeout=30.0)
        await handle.future.stop()

        assert result["stored_payload"] == expected, (
            f"store round-trip failed for {case_name}: got {result['stored_payload']!r}, expected {expected!r}"
        )
        assert result["task_result"] == expected, (
            f"task return value failed for {case_name}: got {result['task_result']!r}, expected {expected!r}"
        )

        task_started = await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.task_started,
            timeout_s=10.0,
            predicate=lambda event: isinstance(event.msg, TaskStarted) and event.msg.task_name == "inspect_payload",
        )
        assert task_started.msg.args == {"payload": expected}, f"task dispatch args failed for {case_name}"  # ty:ignore[unresolved-attribute]


@pytest.mark.asyncio
async def test_serialization_dicts() -> None:
    inner = {
        "name": "dict-case",
        "count": 3,
        "tags": ["a", "b"],
        "nested": {"enabled": True},
    }
    async with _worker_running("dict") as (connection, client):
        workflow_id = str(ulid.ULID())
        handle = await client.start_workflow(
            type=_workflow_type("dict"),
            id=workflow_id,
            input={"payload": inner},
            timeout=timedelta(seconds=30),
        )

        result = await asyncio.wait_for(handle.future, timeout=30.0)
        await handle.future.stop()

        assert result["stored_payload"] == inner, "store round-trip failed for dict"
        assert result["task_result"] == inner, "task return value failed for dict"

        task_started = await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.task_started,
            timeout_s=10.0,
            predicate=lambda event: isinstance(event.msg, TaskStarted) and event.msg.task_name == "inspect_payload",
        )
        assert task_started.msg.args == {"payload": inner}, "task dispatch args failed for dict"  # ty:ignore[unresolved-attribute]


@pytest.mark.asyncio
async def test_serialization_dataclasses_round_trip_as_dicts() -> None:
    inner = {
        "name": "dataclass-case",
        "count": 7,
        "tags": ["alpha", "beta"],
    }
    async with _worker_running("dataclass") as (connection, client):
        workflow_id = str(ulid.ULID())
        handle = await client.start_workflow(
            type=_workflow_type("dataclass"),
            id=workflow_id,
            input={"payload": inner},
            timeout=timedelta(seconds=30),
        )

        result = await asyncio.wait_for(handle.future, timeout=30.0)
        await handle.future.stop()

        assert result["stored_payload"] == inner, "store round-trip failed for dataclass"
        assert result["task_result"] == inner, "task return value failed for dataclass"

        task_started = await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.task_started,
            timeout_s=10.0,
            predicate=lambda event: isinstance(event.msg, TaskStarted) and event.msg.task_name == "inspect_payload",
        )
        assert task_started.msg.args == {"payload": inner}, "task dispatch args failed for dataclass"  # ty:ignore[unresolved-attribute]


@pytest.mark.asyncio
async def test_serialization_msgspec_structs_round_trip_as_dicts() -> None:
    inner = {
        "name": "struct-case",
        "count": 5,
        "tags": ["x", "y"],
    }
    async with _worker_running("struct") as (connection, client):
        workflow_id = str(ulid.ULID())
        handle = await client.start_workflow(
            type=_workflow_type("struct"),
            id=workflow_id,
            input={"payload": inner},
            timeout=timedelta(seconds=30),
        )

        result = await asyncio.wait_for(handle.future, timeout=30.0)
        await handle.future.stop()

        assert result["stored_payload"] == inner, "store round-trip failed for struct"
        assert result["task_result"] == inner, "task return value failed for struct"

        task_started = await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.task_started,
            timeout_s=10.0,
            predicate=lambda event: isinstance(event.msg, TaskStarted) and event.msg.task_name == "inspect_payload",
        )
        assert task_started.msg.args == {"payload": inner}, "task dispatch args failed for struct"  # ty:ignore[unresolved-attribute]


@pytest.mark.asyncio
async def test_serialization_pydantic_typed() -> None:
    inner = {"name": "typed-case", "count": 2, "tags": ["p", "q"]}
    async with _worker_running("pydantic_typed") as (_, client):
        handle = await client.start_workflow(
            type=_workflow_type("pydantic_typed"),
            id=str(ulid.ULID()),
            input={"payload": inner},
            timeout=timedelta(seconds=30),
        )

        result = await asyncio.wait_for(handle.future, timeout=30.0)
        await handle.future.stop()

        assert result["store_value"] == "typed-case", (
            f"workflow result: store round-trip failed: {result['store_value']!r}"
        )


@pytest.mark.asyncio
async def test_serialization_pydantic_task_args() -> None:
    inner = {"name": "pydantic-case", "count": 11, "tags": ["one", "two"]}
    async with _worker_running("pydantic") as (connection, client):
        workflow_id = str(ulid.ULID())
        handle = await client.start_workflow(
            type=_workflow_type("pydantic"),
            id=workflow_id,
            input={"payload": inner},
            timeout=timedelta(seconds=30),
        )

        result = await asyncio.wait_for(handle.future, timeout=30.0)
        await handle.future.stop()

        assert result == inner

        task_started = await _wait_for_history_event(
            js=connection.js,
            manifest=connection.manifest,
            wf_id=workflow_id,
            run_id=handle.run_info.id,
            kind=HistoryKind.task_started,
            timeout_s=10.0,
            predicate=lambda event: isinstance(event.msg, TaskStarted) and event.msg.task_name == "inspect_payload",
        )
        assert task_started.msg.args == {"payload": inner}  # ty:ignore[unresolved-attribute]
