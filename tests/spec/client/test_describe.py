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


def _build_waiting_workflow() -> Workflow:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_describe"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait_for_event(timeout=timedelta(seconds=30))

    @wf.event()
    async def finish(ctx: Context) -> Directive:
        return ctx.next.complete("ok")

    return wf


async def test_describe_returns_run_info_for_started_workflow(worker, grctl_client) -> None:
    wf = _build_waiting_workflow()
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    await wait_for_run_history(grctl_client, wf_id, handle.run_info.id, [HistoryKind.run_started])

    try:
        run_info = await grctl_client.describe(wf_id)

        assert run_info.id == handle.run_info.id
        assert run_info.wf_id == wf_id
        assert run_info.wf_type == wf.workflow_type

        await handle.send("finish")
        assert await asyncio.wait_for(handle.future, timeout=30) == "ok"
    finally:
        await handle.future.stop()


async def test_describe_raises_not_found_for_unknown_wf_id(grctl_client) -> None:
    with pytest.raises(WorkflowNotFoundError):
        await grctl_client.describe(str(ulid.ULID()))
