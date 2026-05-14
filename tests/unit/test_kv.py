import logging
from unittest.mock import AsyncMock

import msgspec
import pytest

from grctl.logging_config import get_logger, setup_logging
from grctl.models import Directive, Start
from grctl.models.directive import DirectiveKind
from grctl.worker.context import Context
from grctl.worker.run_manager import RunManager
from grctl.worker.store import StoreKeyNotFoundError
from grctl.workflow import Workflow
from tests.conftest import create_directive

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


async def test_store_put_sends_kv_updates(mock_connection):
    """Test that store.put() buffers values and sends them in directive's kv_updates."""
    connection, published = mock_connection

    hello_wf = Workflow(workflow_type="Hello")

    @hello_wf.start()
    async def start(ctx: Context, name: str) -> Directive:
        ctx.store.put("name", name)
        ctx.store.put("message", f"Hello, {name}!")
        ctx.store.put("count", 42)
        ctx.store.put("config", {"enabled": True, "timeout": 30})
        return ctx.next.complete(f"Hello, {name}!")

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[hello_wf],
        connection=connection,
    )

    directive = create_directive(
        msg=Start(
            input={"name": "Alice"},
        ),
        kind=DirectiveKind.start,
        directive_id="directive-1",
        run_id="test-run-1",
        wf_id="wf-1",
        wf_type="Hello",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    # Find the directive in published messages
    directive_msg = next((msg for _, msg in published if isinstance(msg, Directive)), None)
    assert directive_msg is not None, "Expected to find a Directive message"

    from grctl.models.directive import StepResult  # noqa: PLC0415

    assert isinstance(directive_msg.msg, StepResult)
    kv_updates = directive_msg.msg.kv_updates

    # Basic shape checks only; values are stored in KV and not asserted here.
    assert "name" in kv_updates, "Expected 'name' in kv_updates"
    assert "message" in kv_updates, "Expected 'message' in kv_updates"
    assert "count" in kv_updates, "Expected 'count' in kv_updates"
    assert "config" in kv_updates, "Expected 'config' in kv_updates"


async def test_store_get_raises_when_not_in_kv(mock_connection):
    """Test that store.get() raises when a value doesn't exist in NATS KV."""
    connection, _published = mock_connection

    kv_workflow = Workflow(workflow_type="KVTest")

    @kv_workflow.start()
    async def start_kv_test(ctx: Context) -> Directive:
        with pytest.raises(StoreKeyNotFoundError, match="nonexistent_key"):
            await ctx.store.get("nonexistent_key")

        ctx.store.put("test_passed", True)
        return ctx.next.complete("done")

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[kv_workflow],
        connection=connection,
    )

    directive = create_directive(
        msg=Start(
            input={},
        ),
        kind=DirectiveKind.start,
        directive_id="directive-1",
        run_id="test-run-1",
        wf_id="wf-1",
        wf_type="KVTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()


async def test_store_get_returns_python_types(mock_connection):
    """Test that store.get() returns Python types not binary."""
    connection, _published = mock_connection

    kv_workflow = Workflow(workflow_type="TypeTest")

    @kv_workflow.start()
    async def start_type_test(ctx: Context) -> Directive:
        # Get values from KV (pre-populated)
        name = await ctx.store.get("stored_name")
        count = await ctx.store.get("stored_count")
        config = await ctx.store.get("stored_config")
        items = await ctx.store.get("stored_list")

        # Verify types
        assert isinstance(name, str), f"Expected str but got {type(name)}"
        assert name == "Bob"

        assert isinstance(count, int), f"Expected int but got {type(count)}"
        assert count == 100

        assert isinstance(config, dict), f"Expected dict but got {type(config)}"
        assert config["active"] is True

        assert isinstance(items, list), f"Expected list but got {type(items)}"
        assert items == [1, 2, 3]

        ctx.store.put("test_passed", True)
        return ctx.next.complete("done")

    # Pre-populate KV with test data (keys will be prefixed with grctl_wf_kv.wf-1.test-run-1.)
    async def mock_kv_get(key: str):
        kv_data = {
            "grctl_wf_kv.wf-1.test-run-1.stored_name": msgspec.msgpack.encode("Bob"),
            "grctl_wf_kv.wf-1.test-run-1.stored_count": msgspec.msgpack.encode(100),
            "grctl_wf_kv.wf-1.test-run-1.stored_config": msgspec.msgpack.encode({"active": True, "level": 5}),
            "grctl_wf_kv.wf-1.test-run-1.stored_list": msgspec.msgpack.encode([1, 2, 3]),
        }
        if key in kv_data:
            entry = AsyncMock()
            entry.value = kv_data[key]
            return entry
        return None

    # Override the KV mock to return pre-populated data
    kv = AsyncMock()
    kv.get = AsyncMock(side_effect=mock_kv_get)
    connection._js.key_value = AsyncMock(return_value=kv)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[kv_workflow],
        connection=connection,
    )

    directive = create_directive(
        msg=Start(
            input={},
        ),
        kind=DirectiveKind.start,
        directive_id="directive-1",
        run_id="test-run-1",
        wf_id="wf-1",
        wf_type="TypeTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()


async def test_store_put_then_get_in_same_step(mock_connection):
    """Test that store.put() stores values and they can be retrieved in the same step."""
    connection, published = mock_connection

    kv_workflow = Workflow(workflow_type="PutTest")

    @kv_workflow.start()
    async def start_put_test(ctx: Context) -> Directive:
        # Put various types of values
        ctx.store.put("string_val", "test_string")
        ctx.store.put("int_val", 123)
        ctx.store.put("dict_val", {"key": "value", "nested": {"a": 1}})
        ctx.store.put("list_val", ["a", "b", "c"])
        ctx.store.put("bool_val", False)

        # Get them back immediately and verify (in-memory cache)
        string_val = await ctx.store.get("string_val")
        assert string_val == "test_string"

        int_val = await ctx.store.get("int_val")
        assert int_val == 123

        dict_val = await ctx.store.get("dict_val")
        assert dict_val == {"key": "value", "nested": {"a": 1}}

        list_val = await ctx.store.get("list_val")
        assert list_val == ["a", "b", "c"]

        bool_val = await ctx.store.get("bool_val")
        assert bool_val is False

        return ctx.next.complete("done")

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[kv_workflow],
        connection=connection,
    )

    directive = create_directive(
        msg=Start(
            input={},
        ),
        kind=DirectiveKind.start,
        directive_id="directive-1",
        run_id="test-run-1",
        wf_id="wf-1",
        wf_type="PutTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    # Find the directive in published messages
    directive_msg = next((msg for _, msg in published if isinstance(msg, Directive)), None)
    assert directive_msg is not None, "Expected to find a Directive message"

    from grctl.models.directive import StepResult  # noqa: PLC0415

    assert isinstance(directive_msg.msg, StepResult)
    kv_updates = directive_msg.msg.kv_updates

    # Verify all 5 values were sent in kv_updates
    assert len(kv_updates) == 5
    assert "string_val" in kv_updates
    assert "int_val" in kv_updates
    assert "dict_val" in kv_updates
    assert "list_val" in kv_updates
    assert "bool_val" in kv_updates
