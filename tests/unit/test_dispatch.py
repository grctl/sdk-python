import dataclasses
from unittest.mock import AsyncMock, MagicMock, patch

import msgspec
import pytest
from pydantic import BaseModel

from grctl.worker.codec import CodecRegistry
from grctl.worker.runner import WorkflowRunner
from grctl.worker.runtime import _step_run_time
from grctl.workflow.workflow import HandlerConfig, HandlerSpec


def make_config(handler, params):
    return HandlerConfig(handler=handler, spec=HandlerSpec(params=params))


@pytest.fixture
def runner():
    rt = MagicMock()
    rt.codec = CodecRegistry()
    rt.step_history = [MagicMock()]  # non-empty skips _publish_step_started_event
    rt.get_step_context.return_value = MagicMock()
    r = WorkflowRunner(rt)
    yield r
    _step_run_time.reset(r._runtime_token)


@pytest.mark.asyncio
async def test_no_params_calls_handler_with_ctx_only(runner):
    received = {}

    async def handler(ctx):
        received["called"] = True

    with patch.object(runner, "_publish_next_directive", new=AsyncMock()):
        await runner._execute_step(make_config(handler, {}), None)

    assert received["called"] is True


@pytest.mark.asyncio
async def test_multiple_typed_params_each_received_correctly(runner):
    received = {}

    async def handler(ctx, name: str, count: int):
        received["name"] = name
        received["count"] = count

    with patch.object(runner, "_publish_next_directive", new=AsyncMock()):
        await runner._execute_step(
            make_config(handler, {"name": str, "count": int}),
            {"name": "Alice", "count": 5},
        )

    assert received["name"] == "Alice"
    assert received["count"] == 5


@pytest.mark.asyncio
async def test_missing_required_param_raises_key_error(runner):
    # Multi-param: missing key in payload dict raises KeyError
    async def handler(ctx, x: int, y: str): ...

    with pytest.raises(KeyError):
        await runner._execute_step(make_config(handler, {"x": int, "y": str}), {"y": "hello"})


@pytest.mark.asyncio
async def test_wrong_type_raises_validation_error(runner):
    # Multi-param: wrong type for a key raises ValidationError
    async def handler(ctx, x: int, y: str): ...

    with pytest.raises(msgspec.ValidationError):
        await runner._execute_step(make_config(handler, {"x": int, "y": str}), {"x": "not-an-int", "y": "ok"})


@pytest.mark.asyncio
async def test_single_pydantic_param_receives_instance(runner):
    class UserModel(BaseModel):
        id: str
        name: str

    received = {}

    async def handler(ctx, user: UserModel):
        received["user"] = user

    with patch.object(runner, "_publish_next_directive", new=AsyncMock()):
        await runner._execute_step(
            make_config(handler, {"user": UserModel}),
            {"id": "1", "name": "Alice"},
        )

    assert isinstance(received["user"], UserModel)
    assert received["user"].id == "1"
    assert received["user"].name == "Alice"


@pytest.mark.asyncio
async def test_single_dataclass_param_receives_instance(runner):
    @dataclasses.dataclass
    class Point:
        x: int
        y: int

    received = {}

    async def handler(ctx, point: Point):
        received["point"] = point

    with patch.object(runner, "_publish_next_directive", new=AsyncMock()):
        await runner._execute_step(
            make_config(handler, {"point": Point}),
            {"x": 3, "y": 4},
        )

    assert isinstance(received["point"], Point)
    assert received["point"].x == 3
    assert received["point"].y == 4
