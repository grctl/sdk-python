import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.client import Client
from grctl.models import RunStatus
from grctl.models.errors import WorkflowError
from grctl.worker import ChildOutcome, Context
from grctl.workflow import Directive, Workflow
from tests.spec.workflows import unique_workflow_type

_WORKFLOW_TIMEOUT = timedelta(seconds=60)


async def test_failed_child_raises_workflow_error_on_parent_step(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_failure_raises_parent")
    child_wf_type = unique_workflow_type("spec_child_failure_raises_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        raise ValueError("child failed")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child")
        await handle.future
        return ctx.next.complete("unreachable")

    await worker([parent_wf, child_wf])

    with pytest.raises(WorkflowError):
        await grctl_client.run_workflow(
            type=parent_wf_type,
            id=str(ulid.ULID()),
            input={},
            timeout=_WORKFLOW_TIMEOUT,
        )


async def test_child_failure_message_is_preserved(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_failure_message_parent")
    child_wf_type = unique_workflow_type("spec_child_failure_message_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        raise ValueError("specific child error message")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child")
        await handle.future
        return ctx.next.complete("unreachable")

    await worker([parent_wf, child_wf])

    with pytest.raises(WorkflowError, match="specific child error message"):
        await grctl_client.run_workflow(
            type=parent_wf_type,
            id=str(ulid.ULID()),
            input={},
            timeout=_WORKFLOW_TIMEOUT,
        )


async def test_child_step_timeout_triggers_parent_on_completed_callback(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_step_timeout_cb_parent")
    child_wf_type = unique_workflow_type("spec_child_step_timeout_cb_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.step(child_blocking_step)

    @child_wf.step(timeout=timedelta(seconds=0.1))
    async def child_blocking_step(ctx: Context) -> Directive:
        await asyncio.sleep(60)
        return ctx.next.complete("unreachable")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child", on_completed_step=on_child_done)
        return ctx.next.wait()

    @parent_wf.step()
    async def on_child_done(ctx: Context, outcome: ChildOutcome) -> Directive:
        assert not outcome.ok
        assert outcome.status == RunStatus.failed
        assert outcome.error is not None
        assert outcome.error.type == "StepTimeout"
        return ctx.next.complete("parent-received-timeout-callback")

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "parent-received-timeout-callback"
