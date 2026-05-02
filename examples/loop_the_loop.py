import asyncio
import logging
from datetime import timedelta

import ulid

from grctl.client import Client, Connection, get_logger, setup_logging
from grctl.worker import Context, Worker, task
from grctl.workflow import Directive, Workflow

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


ltl = Workflow(workflow_type="LoopTheLoop")


@task
async def incr(c: int) -> int:
    return c + 1


@ltl.start()
async def start(ctx: Context, start: int) -> Directive:
    ctx.store.put("c", start)
    return ctx.next.step(incr_step)


@ltl.step()
async def incr_step(ctx: Context) -> Directive:
    c = await ctx.store.get("c", int)

    for _ in range(10):
        c = await incr(c)
        ctx.store.put("c", c)

    if c >= 1000:  # noqa: PLR2004
        return ctx.next.complete(c)

    return ctx.next.step(incr_step)


async def main() -> None:
    """Run a workflow that loops through repeated steps until completion."""
    logger.info("Starting worker...")
    connection = await Connection.connect()
    worker = Worker(
        workflows=[ltl],
        connection=connection,
    )

    worker_task = asyncio.create_task(worker.start())

    logger.info("Worker is ready. Sending test request...")

    client = Client(connection=connection)

    try:
        result = await client.run_workflow(
            type=ltl.workflow_type,
            id=str(ulid.ULID()),
            input={"start": 0},
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
