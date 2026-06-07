from datetime import timedelta

import ulid

from grctl.client import Client
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.workflows import unique_workflow_type

_WORKFLOW_TIMEOUT = timedelta(seconds=60)


async def test_child_sends_result_to_parent_via_send_to_parent(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_result_pattern1_parent")
    child_wf_type = unique_workflow_type("spec_child_result_pattern1_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.step(child_send)

    @child_wf.step()
    async def child_send(ctx: Context) -> Directive:
        await ctx.send_to_parent("result_ready", payload="child-result")
        return ctx.next.complete("child-done")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child")
        return ctx.next.wait()

    @parent_wf.event(name="result_ready")
    async def on_result(ctx: Context, payload: str) -> Directive:
        return ctx.next.complete(payload)

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "child-result"


async def test_run_child_returns_child_result(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_run_child_success_parent")
    child_wf_type = unique_workflow_type("spec_run_child_success_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.complete("child-result")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        result = await ctx.run_child(child_wf_type, f"{ctx.run.wf_id}-child")
        return ctx.next.complete(result)

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "child-result"


async def test_parent_can_await_child_future_in_same_step(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_result_pattern2_parent")
    child_wf_type = unique_workflow_type("spec_child_result_pattern2_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.complete("child-result")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        handle = await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child")
        result = await handle.future
        return ctx.next.complete(result)

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "child-result"
