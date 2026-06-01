from datetime import timedelta

import ulid
from pydantic import BaseModel

from grctl.client import Client
from grctl.worker import ChildOutcome, Context
from grctl.workflow import Directive, Workflow
from tests.spec.workflows import unique_workflow_type

_WORKFLOW_TIMEOUT = timedelta(seconds=60)


class ChildResult(BaseModel):
    value: str
    count: int


async def test_on_completed_step_receives_child_result(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_cb_success_parent")
    child_wf_type = unique_workflow_type("spec_child_cb_success_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.complete("child-result")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child", on_completed_step=on_done)
        return ctx.next.wait()

    @parent_wf.step()
    async def on_done(ctx: Context, outcome: ChildOutcome) -> Directive:
        assert outcome.ok
        return ctx.next.complete(outcome.result)

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "child-result"


async def test_on_completed_step_receives_child_error(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_cb_failure_parent")
    child_wf_type = unique_workflow_type("spec_child_cb_failure_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        raise RuntimeError("child boom")

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child", on_completed_step=on_done)
        return ctx.next.wait()

    @parent_wf.step()
    async def on_done(ctx: Context, outcome: ChildOutcome) -> Directive:
        assert not outcome.ok
        assert outcome.error is not None
        return ctx.next.complete(outcome.error.message)

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert "boom" in result


async def test_on_completed_step_receives_pydantic_result(worker, grctl_client: Client) -> None:
    parent_wf_type = unique_workflow_type("spec_child_cb_pydantic_parent")
    child_wf_type = unique_workflow_type("spec_child_cb_pydantic_child")

    child_wf = Workflow(workflow_type=child_wf_type)
    parent_wf = Workflow(workflow_type=parent_wf_type)

    @child_wf.start()
    async def child_start(ctx: Context) -> Directive:
        return ctx.next.complete(ChildResult(value="child-result", count=3))

    @parent_wf.start()
    async def parent_start(ctx: Context) -> Directive:
        await ctx.start_child(child_wf_type, f"{ctx.run.wf_id}-child", on_completed_step=on_done)
        return ctx.next.wait()

    @parent_wf.step()
    async def on_done(ctx: Context, outcome: ChildOutcome[ChildResult]) -> Directive:
        assert outcome.ok
        assert isinstance(outcome.result, ChildResult)
        assert outcome.result.value == "child-result"
        assert outcome.result.count == 3
        return ctx.next.complete(outcome.result.value)

    await worker([parent_wf, child_wf])

    result = await grctl_client.run_workflow(
        type=parent_wf_type,
        id=str(ulid.ULID()),
        input={},
        timeout=_WORKFLOW_TIMEOUT,
    )

    assert result == "child-result"
