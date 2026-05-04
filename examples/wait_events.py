import asyncio
import logging
from datetime import timedelta

import ulid

from grctl.client import Client, Connection, setup_logging
from grctl.worker import Context, StoreKeyNotFoundError, Worker, task
from grctl.workflow import Directive, Workflow

setup_logging(level=logging.DEBUG)
logger = logging.getLogger(__name__)


greet_events = Workflow(workflow_type="GreetEvents")


@task
async def call_greeting_api(name: str) -> str:
    logger.info(f"Calling external Greeting API for name: {name}")
    await asyncio.sleep(0.1)
    return f"Hello, {name}!"


@task
async def call_farewell_api(name: str) -> str:
    logger.info(f"Calling external Farewell API for name: {name}")
    await asyncio.sleep(0.1)
    return f"Goodbye, {name}!"


@greet_events.start()
async def start(ctx: Context, name: str) -> Directive:
    ctx.store.put("name", name)
    logger.info(f"Initialized workflow for: {name}")
    return ctx.next.wait_for_event()


@greet_events.event()
async def greet(ctx: Context, title: str) -> Directive:
    name = await ctx.store.get("name", str)
    greeting = await call_greeting_api(f"{title} {name}")
    ctx.store.put("greeting", greeting)
    ctx.store.put("message", greeting)
    return ctx.next.wait_for_event()


@greet_events.event()
async def farewell(ctx: Context, farewell_note: str) -> Directive:
    try:
        greeting = await ctx.store.get("greeting", str)
    except StoreKeyNotFoundError as e:
        raise ValueError("greet event must be handled before farewell") from e

    name = await ctx.store.get("name", str)

    res = await call_farewell_api(name)

    message = f"{greeting} {res} {farewell_note}"
    ctx.store.put("message", message)

    logger.info(f"Final message: {message}")
    return ctx.next.complete(message)


async def main() -> None:
    """Run a workflow that waits for multiple events before completing."""
    logger.info("Starting worker...")
    connection = await Connection.connect()
    worker = Worker(
        workflows=[greet_events],
        connection=connection,
    )

    worker_task = asyncio.create_task(worker.start())

    logger.info("Worker is ready. Sending test request...")

    client = Client(connection=connection)

    try:
        wf_handle = await client.start_workflow(
            type=greet_events.workflow_type,
            id=str(ulid.ULID()),
            input={"name": "Cem"},
            timeout=timedelta(seconds=30),
        )

        await asyncio.sleep(1)
        await wf_handle.send("greet", {"title": "Mr"})

        await asyncio.sleep(1)
        await wf_handle.send("farewell", {"farewell_note": "Until next time!"})

        result = await asyncio.wait_for(wf_handle.future, timeout=120)
        logger.info(f"Workflow result: {result}")
    except Exception:
        logger.exception("Workflow execution failed")
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
            await asyncio.gather(worker_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
