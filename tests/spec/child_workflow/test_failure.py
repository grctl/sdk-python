from datetime import timedelta

import pytest
import ulid

from grctl.client import Client
from grctl.models.errors import WorkflowError
from grctl.worker import Context
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
        handle = await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
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
        handle = await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
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
