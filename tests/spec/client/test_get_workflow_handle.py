import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowNotFoundError
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.client.helpers import wait_for_run_history


def _unique_wf_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


def _build_attach_workflow() -> Workflow:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_handle_attach"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait_for_event(timeout=timedelta(seconds=30))

    @wf.event()
    async def finish(ctx: Context, result: str) -> Directive:
        return ctx.next.complete(result)

    return wf


async def test_get_workflow_handle_attaches_and_resolves_result(worker, grctl_client) -> None:
    wf = _build_attach_workflow()
    await worker([wf])

    wf_id = str(ulid.ULID())
    original_handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    await wait_for_run_history(grctl_client, wf_id, original_handle.run_info.id, [HistoryKind.run_started])

    attached_handle = await grctl_client.get_workflow_handle(wf_id)

    try:
        assert attached_handle.run_info.id == original_handle.run_info.id
        assert attached_handle.run_info.wf_id == wf_id
        assert attached_handle.run_info.wf_type == wf.workflow_type

        await attached_handle.send("finish", {"result": "attached-ok"})

        result = await asyncio.wait_for(attached_handle.future, timeout=30)
        assert result == "attached-ok"
        assert await asyncio.wait_for(original_handle.future, timeout=5) == "attached-ok"
    finally:
        await attached_handle.future.stop()
        await original_handle.future.stop()


async def test_get_workflow_handle_raises_not_found_for_unknown_wf_id(grctl_client) -> None:
    with pytest.raises(WorkflowNotFoundError):
        await grctl_client.get_workflow_handle(str(ulid.ULID()))
