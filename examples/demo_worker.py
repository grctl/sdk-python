"""Demo worker — registers Hello and Payment workflows.

Usage:
    uv run python examples/demo_worker.py
"""

import asyncio
import logging

from examples.child_workflow import payment_wf
from examples.hello_world import hello
from grctl.client import Connection, setup_logging
from grctl.worker import Worker

setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Connecting to grctld...")
    connection = await Connection.connect()
    worker = Worker(workflows=[hello, payment_wf], connection=connection)
    logger.info("Worker started — registered workflows: Hello, Payment")
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
