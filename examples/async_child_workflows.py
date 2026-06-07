import asyncio
import logging
import uuid
from datetime import timedelta

import ulid

from grctl.client import Client, Connection, setup_logging
from grctl.worker import Context, StoreKeyNotFoundError, Worker, task
from grctl.workflow import Directive, Workflow

setup_logging(level=logging.DEBUG)
logger = logging.getLogger(__name__)


task_a_wf = Workflow(workflow_type="TaskA")
task_b_wf = Workflow(workflow_type="TaskB")
orchestrator_wf = Workflow(workflow_type="Orchestrator")


@task
async def compute_a(input_data: str) -> str:
    logger.info(f"Task A computing with input: {input_data}")
    await asyncio.sleep(0.2)
    return f"result-a-{uuid.uuid4().hex[:6]}"


@task
async def compute_b(input_data: str) -> str:
    logger.info(f"Task B computing with input: {input_data}")
    await asyncio.sleep(0.3)
    return f"result-b-{uuid.uuid4().hex[:6]}"


# ============================================================================
# TASK A WORKFLOW (Child 1)
# ============================================================================


@task_a_wf.start()
async def task_a_start(ctx: Context, input_data: str) -> Directive:
    ctx.store.put("input_data", input_data)
    ctx.logger.info(f"Task A workflow started with input: {input_data}")
    return ctx.next.step(task_a_process)


@task_a_wf.step()
async def task_a_process(ctx: Context) -> Directive:
    input_data = await ctx.store.get("input_data", str)
    result = await compute_a(input_data)
    ctx.store.put("result", result)
    ctx.logger.info(f"Task A completed with result: {result}")
    await ctx.send_to_parent(event_name="task_a_completed", payload={"result": result})
    return ctx.next.complete({"result": result})


# ============================================================================
# TASK B WORKFLOW (Child 2)
# ============================================================================


@task_b_wf.start()
async def task_b_start(ctx: Context, input_data: str) -> Directive:
    ctx.store.put("input_data", input_data)
    ctx.logger.info(f"Task B workflow started with input: {input_data}")
    return ctx.next.step(task_b_process)


@task_b_wf.step()
async def task_b_process(ctx: Context) -> Directive:
    input_data = await ctx.store.get("input_data", str)
    result = await compute_b(input_data)
    ctx.store.put("result", result)
    ctx.logger.info(f"Task B completed with result: {result}")
    await ctx.send_to_parent(event_name="task_b_completed", payload={"result": result})
    return ctx.next.complete({"result": result})


# ============================================================================
# ORCHESTRATOR WORKFLOW (Parent)
# ============================================================================


@orchestrator_wf.start()
async def orchestrator_start(ctx: Context, input_a: str, input_b: str) -> Directive:
    ctx.store.put("input_a", input_a)
    ctx.store.put("input_b", input_b)
    ctx.logger.info(f"Orchestrator starting children with inputs: {input_a}, {input_b}")

    task_a_id = f"task-a-{ulid.ULID()}"
    task_b_id = f"task-b-{ulid.ULID()}"

    handle_a = await ctx.start_child(
        task_a_wf.workflow_type,
        workflow_id=task_a_id,
        workflow_input={"input_data": input_a},
        workflow_timeout=timedelta(minutes=1),
    )
    ctx.logger.info("Started child Task A: %s", handle_a.run_info.id)

    handle_b = await ctx.start_child(
        task_b_wf.workflow_type,
        workflow_id=task_b_id,
        workflow_input={"input_data": input_b},
        workflow_timeout=timedelta(minutes=1),
    )
    ctx.logger.info("Started child Task B: %s", handle_b.run_info.id)

    return ctx.next.wait()


@orchestrator_wf.event(name="task_a_completed")
async def on_task_a_completed(ctx: Context, result: str) -> Directive:
    ctx.logger.info("Received Task A result: %s", result)
    ctx.store.put("task_a_result", result)

    try:
        await ctx.store.get("task_b_result", str)
    except StoreKeyNotFoundError:
        return ctx.next.wait()

    return await _finish_orchestrator(ctx)


@orchestrator_wf.event(name="task_b_completed")
async def on_task_b_completed(ctx: Context, result: str) -> Directive:
    ctx.logger.info("Received Task B result: %s", result)
    ctx.store.put("task_b_result", result)

    try:
        await ctx.store.get("task_a_result", str)
    except StoreKeyNotFoundError:
        return ctx.next.wait()

    return await _finish_orchestrator(ctx)


async def _finish_orchestrator(ctx: Context) -> Directive:
    input_a = await ctx.store.get("input_a", str)
    input_b = await ctx.store.get("input_b", str)
    task_a_result = await ctx.store.get("task_a_result", str)
    task_b_result = await ctx.store.get("task_b_result", str)

    message = f"Orchestrator completed: input_a={input_a} -> {task_a_result}, input_b={input_b} -> {task_b_result}"
    ctx.store.put("message", message)
    ctx.logger.info("Final message: %s", message)
    return ctx.next.complete(message)


async def main() -> None:
    """Run an orchestrator workflow that starts two child workflows concurrently."""
    logger.info("Starting worker...")
    connection = await Connection.connect()
    worker = Worker(
        workflows=[orchestrator_wf, task_a_wf, task_b_wf],
        connection=connection,
    )

    worker_task = asyncio.create_task(worker.start())

    logger.info("Worker is ready. Sending test request...")

    client = Client(connection=connection)

    try:
        handle = await client.start_workflow(
            type=orchestrator_wf.workflow_type,
            id=str(ulid.ULID()),
            input={"input_a": "hello", "input_b": "world"},
            timeout=timedelta(seconds=60),
        )
        logger.info(f"Orchestrator workflow started with handle: {handle}")

        result = await asyncio.wait_for(handle.future, timeout=10)
        logger.info(f"Orchestrator workflow result: {result}")

    except Exception:
        logger.exception("Workflow execution failed")
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
