import asyncio
import logging
from datetime import timedelta

import ulid

from grctl.client import Client, Connection, get_logger, setup_logging
from grctl.worker import Context, Worker, task
from grctl.workflow import Directive, Workflow

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


lte = Workflow(workflow_type="LoopTheEvent")


@task
async def incr(c: int) -> int:
    return c + 1


@lte.start()
async def start(ctx: Context, start_count: int) -> Directive:
    ctx.store.put("c", start_count)
    return ctx.next.wait_for_event()


@lte.event()
async def incr_step(ctx: Context) -> Directive:
    c = await ctx.store.get("c", int)

    for _ in range(10):
        c = await incr(c)
        ctx.store.put("c", c)

    if c >= 1000:  # noqa: PLR2004
        return ctx.next.complete(c)

    return ctx.next.wait_for_event()


async def main() -> None:
    """Run a workflow that advances in response to repeated events."""
    logger.info("Starting worker...")
    connection = await Connection.connect()
    worker = Worker(
        workflows=[lte],
        connection=connection,
    )

    worker_task = asyncio.create_task(worker.start())
    logger.info("Worker is ready. Sending test events...")

    try:
        client = Client(connection=connection)
        wf_handle = await client.start_workflow(
            type=lte.workflow_type,
            id=str(ulid.ULID()),
            input={"start_count": 0},
            timeout=timedelta(seconds=30),
        )
        for _ in range(100):
            await wf_handle.send("incr_step")

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
