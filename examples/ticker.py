import asyncio
import logging
import sys
from datetime import timedelta

from grctl.client import Client, Connection, get_logger, setup_logging
from grctl.worker import Context, Worker, task
from grctl.workflow import Directive, Workflow

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


ticker = Workflow(workflow_type="Ticker")

WORKFLOW_ID = "ticker-workflow"
ITERATIONS = 10


@task
async def tick(iteration: int) -> int:
    await asyncio.sleep(1)
    return iteration


@ticker.start()
async def start(ctx: Context, iterations: int) -> Directive:
    ctx.logger.info(f"Starting tick step with {iterations} iterations")

    ctx.store.put("iterations", iterations)
    return ctx.next.step(tick_step)


@ticker.step(timeout=timedelta(seconds=20))
async def tick_step(ctx: Context) -> Directive:
    iterations = await ctx.store.get("iterations", int)

    for i in range(iterations):
        await tick(i)
        ctx.logger.info(f"Tick iteration: {i}")

    return ctx.next.complete(iterations)


async def run_worker() -> None:
    logger.info("Starting worker...")
    connection = await Connection.connect()
    worker = Worker(workflows=[ticker], connection=connection)

    try:
        await worker.start()
    except asyncio.CancelledError:
        logger.info("Worker stopped.")
        await worker.stop()


async def run_start() -> None:
    logger.info(f"Starting workflow {WORKFLOW_ID}...")
    connection = await Connection.connect()

    client = Client(connection=connection)
    await client.start_workflow(
        type=ticker.workflow_type,
        id=WORKFLOW_ID,
        input={"iterations": ITERATIONS},
        timeout=timedelta(seconds=300),
    )
    logger.info(f"Workflow {WORKFLOW_ID} submitted.")
    await connection.close()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "worker"

    if mode == "start":
        asyncio.run(run_start())
    elif mode == "worker":
        asyncio.run(run_worker())
    else:
        raise SystemExit("Usage: python -m sdk_python.examples.ticker [worker|start]")
