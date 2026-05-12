import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowAlreadyRunningError, WorkflowError
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.client.helpers import wait_for_run_history


def _unique_wf_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


def _build_waiting_workflow() -> Workflow:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_start_duplicate"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait_for_event(timeout=timedelta(seconds=1))

    return wf


async def test_start_workflow_returns_handle_with_run_info(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_start_handle"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete("ok")

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        assert handle.run_info.wf_id == wf_id
        assert handle.run_info.wf_type == wf.workflow_type
        assert handle.run_info.id
    finally:
        await handle.future.stop()


async def test_start_workflow_future_resolves_with_result(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_start_result"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete("ok")

    await worker([wf])

    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert await asyncio.wait_for(handle.future, timeout=30) == "ok"


async def test_start_workflow_future_raises_workflow_error_on_failure(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_start_failure"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        raise ValueError("workflow exploded")

    await worker([wf])

    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    with pytest.raises(WorkflowError, match="ValueError: workflow exploded"):
        await asyncio.wait_for(handle.future, timeout=30)


async def test_start_workflow_raises_when_wf_id_already_active(worker, grctl_client) -> None:
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
        with pytest.raises(WorkflowAlreadyRunningError):
            await grctl_client.start_workflow(
                type=wf.workflow_type,
                id=wf_id,
                input={},
                timeout=timedelta(seconds=30),
            )
    finally:
        await handle.future.stop()
