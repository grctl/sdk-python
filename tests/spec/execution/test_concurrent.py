import asyncio
from datetime import timedelta

import ulid

from grctl.worker import Context, task
from grctl.workflow import Directive, Workflow
from tests.spec.workflows import unique_workflow_type

_active = 0
_peak = 0


@task
async def step_one_task(name: str, value: int) -> str:
    global _active, _peak  # noqa: PLW0603
    _active += 1
    _peak = max(_peak, _active)
    await asyncio.sleep(0.05)
    _active -= 1
    return f"one:{name}:{value}"


@task
async def step_two_task(name: str, value: int) -> str:
    global _active, _peak  # noqa: PLW0603
    _active += 1
    _peak = max(_peak, _active)
    await asyncio.sleep(0.05)
    _active -= 1
    return f"two:{name}:{value}"


@task
async def step_three_task(name: str, value: int) -> str:
    global _active, _peak  # noqa: PLW0603
    _active += 1
    _peak = max(_peak, _active)
    await asyncio.sleep(0.05)
    _active -= 1
    return f"three:{name}:{value}"


def make_three_step_workflow(prefix: str = "spec_three_step") -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type(prefix))

    @wf.start()
    async def start(ctx: Context, name: str, value: int) -> Directive:
        ctx.store.put("name", name)
        ctx.store.put("value", value)
        result = await step_one_task(name, value)
        ctx.store.put("step_one", result)
        return ctx.next.step(second_step)

    @wf.step()
    async def second_step(ctx: Context) -> Directive:
        name = await ctx.store.get("name", str)
        value = await ctx.store.get("value", int)
        result = await step_two_task(name, value)
        ctx.store.put("step_two", result)
        return ctx.next.step(third_step)

    @wf.step()
    async def third_step(ctx: Context) -> Directive:
        name = await ctx.store.get("name", str)
        value = await ctx.store.get("value", int)
        result = await step_three_task(name, value)
        return ctx.next.complete(result)

    return wf


async def test_two_concurrent_instances_of_same_workflow_both_complete(worker, grctl_client) -> None:
    global _active, _peak  # noqa: PLW0603
    _active = 0
    _peak = 0

    wf = make_three_step_workflow(prefix="spec_concurrent_two_instances")
    await worker([wf])

    (handle_a, handle_b) = await asyncio.gather(
        grctl_client.start_workflow(
            type=wf.workflow_type,
            id=str(ulid.ULID()),
            input={"name": "same", "value": 42},
            timeout=timedelta(seconds=30),
        ),
        grctl_client.start_workflow(
            type=wf.workflow_type,
            id=str(ulid.ULID()),
            input={"name": "same", "value": 42},
            timeout=timedelta(seconds=30),
        ),
    )

    (result_a, result_b) = await asyncio.gather(
        asyncio.wait_for(handle_a.future, timeout=30),
        asyncio.wait_for(handle_b.future, timeout=30),
    )

    assert result_a == "three:same:42"
    assert result_b == "three:same:42"
    assert _peak >= 2, f"Workflows never ran concurrently (peak active tasks: {_peak})"
