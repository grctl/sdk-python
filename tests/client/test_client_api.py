from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import msgspec
import pytest

from grctl.client.client import Client
from grctl.models import GrctlAPIResponse, HistoryEvent, HistoryKind, RunInfo, RunStarted
from grctl.models.errors import WorkflowError, WorkflowNotFoundError


@pytest.mark.asyncio
async def test_run_workflow_rejects_old_keyword_names() -> None:
    client = Client(connection=MagicMock())
    kwargs: dict[str, Any] = {
        "workflow_type": "Example",
        "workflow_id": "example-1",
        "workflow_input": {},
        "workflow_timeout": None,
    }

    with pytest.raises(TypeError, match="workflow_type"):
        await client.run_workflow(**kwargs)


@pytest.mark.asyncio
async def test_start_workflow_rejects_old_keyword_names() -> None:
    client = Client(connection=MagicMock())
    kwargs: dict[str, Any] = {
        "workflow_type": "Example",
        "workflow_id": "example-1",
        "workflow_input": {},
        "workflow_timeout": None,
    }

    with pytest.raises(TypeError, match="workflow_type"):
        await client.start_workflow(**kwargs)


@pytest.mark.asyncio
async def test_describe_returns_run_info() -> None:
    connection = MagicMock()
    connection.publisher.publish_cmd = AsyncMock(
        return_value=msgspec.msgpack.encode(
            GrctlAPIResponse(
                success=True,
                payload=msgspec.Raw(msgspec.msgpack.encode(RunInfo(id="run-1", wf_id="wf-1", wf_type="Example"))),
            )
        )
    )
    client = Client(connection=connection)

    run_info = await client.describe("wf-1")

    assert run_info.id == "run-1"
    assert run_info.wf_id == "wf-1"


@pytest.mark.asyncio
async def test_describe_raises_workflow_not_found() -> None:
    connection = MagicMock()
    connection.publisher.publish_cmd = AsyncMock(
        return_value=msgspec.msgpack.encode(
            {
                "success": False,
                "payload": b"",
                "error": {"code": 4002, "message": "not found", "detail": ""},
            }
        )
    )
    client = Client(connection=connection)

    with pytest.raises(WorkflowNotFoundError, match="workflow 'wf-1' not found: not found"):
        await client.describe("wf-1")


@pytest.mark.asyncio
async def test_describe_raises_workflow_error_for_other_api_failures() -> None:
    connection = MagicMock()
    connection.publisher.publish_cmd = AsyncMock(
        return_value=msgspec.msgpack.encode(
            {
                "success": False,
                "payload": b"",
                "error": {"code": 5001, "message": "boom", "detail": ""},
            }
        )
    )
    client = Client(connection=connection)

    with pytest.raises(WorkflowError, match=r"describe failed \(code=5001\): boom"):
        await client.describe("wf-1")


@pytest.mark.asyncio
async def test_get_history_resolves_latest_run_via_describe() -> None:
    event = HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.run_started,
        msg=RunStarted(input={"value": 1}),
    )
    connection = MagicMock()
    connection.js = MagicMock()
    connection.manifest = MagicMock()
    client = Client(connection=connection)

    with (
        patch.object(
            client, "describe", new=AsyncMock(return_value=RunInfo(id="run-1", wf_id="wf-1", wf_type="Example"))
        ),
        patch("grctl.client.client.fetch_run_history", new=AsyncMock(return_value=[event])) as fetch_run_history,
    ):
        history = await client.get_history("wf-1")

    assert history == [event]
    fetch_run_history.assert_awaited_once_with(
        js=connection.js,
        manifest=connection.manifest,
        wf_id="wf-1",
        run_id="run-1",
    )


@pytest.mark.asyncio
async def test_get_history_uses_explicit_run_id_without_describe() -> None:
    connection = MagicMock()
    connection.js = MagicMock()
    connection.manifest = MagicMock()
    client = Client(connection=connection)

    with (
        patch.object(client, "describe", new=AsyncMock()) as describe,
        patch("grctl.client.client.fetch_run_history", new=AsyncMock(return_value=[])) as fetch_run_history,
    ):
        history = await client.get_history("wf-1", run_id="run-2")

    assert history == []
    describe.assert_not_awaited()
    fetch_run_history.assert_awaited_once_with(
        js=connection.js,
        manifest=connection.manifest,
        wf_id="wf-1",
        run_id="run-2",
    )
