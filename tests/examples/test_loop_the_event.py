import asyncio
import contextlib
import logging
from datetime import timedelta

import pytest
import ulid

from examples.loop_the_event import lte
from grctl.client.client import Client
from grctl.logging_config import get_logger, setup_logging
from grctl.nats.connection import Connection
from grctl.worker.worker import Worker

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


@pytest.mark.asyncio
async def test_wait_events_example_end_to_end() -> None:
    """Run wait_events example workflow against a live server."""
    connection = await Connection.connect()
    worker = Worker(workflows=[lte], connection=connection)
    worker_task = asyncio.create_task(worker.start())
    client = Client(connection=connection)

    await asyncio.sleep(0.05)

    try:
        workflow_id = str(ulid.ULID())
        name = "Integration Tester"

        wf_handle = await client.start_workflow(
            type=lte.workflow_type,
            id=workflow_id,
            input={"start_count": 900},
            timeout=timedelta(seconds=300),
        )

        for _ in range(10):
            await wf_handle.send("incr_step")

        result = await asyncio.wait_for(wf_handle.future, timeout=30)

        logger.info(f"Workflow result: {result}")
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        Connection.reset()
