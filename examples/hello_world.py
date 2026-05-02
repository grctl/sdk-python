import asyncio
import logging
from datetime import timedelta

import ulid

from grctl.client import Client, Connection, get_logger, setup_logging
from grctl.worker import Context, Worker, task
from grctl.workflow import Directive, Workflow

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


hello = Workflow(workflow_type="Hello")


@task
async def call_greeting_api(name: str) -> str:
    logger.info(f"Calling external Greeting API for name: {name}")
    return f"Hello, {name}!"


@hello.start()
async def start(ctx: Context, name: str) -> Directive:
    logger.info(f"Initialized workflow for: {name}")
    ctx.store.put("name", name)
    greeting = await call_greeting_api(name)
    message = f"{greeting}"
    ctx.store.put("message", message)

    return ctx.next.complete(message)


async def main() -> None:
    """Run a simple workflow from start to completion."""
    logger.info("Starting worker...")
    connection = await Connection.connect()
    worker = Worker(
        workflows=[hello],
        connection=connection,
    )

    worker_task = asyncio.create_task(worker.start())

    logger.info("Worker is ready. Sending test request...")

    client = Client(connection=connection)

    try:
        result = await client.run_workflow(
            type=hello.workflow_type,
            id=str(ulid.ULID()),
            input={"name": "World"},
            timeout=timedelta(seconds=30),
        )
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
