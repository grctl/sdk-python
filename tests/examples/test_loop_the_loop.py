import asyncio
import contextlib
from datetime import timedelta

import pytest
import ulid

from examples.loop_the_loop import ltl
from grctl.client.client import Client
from grctl.nats.connection import Connection
from grctl.worker.worker import Worker


@pytest.mark.asyncio
async def test_ltl_example() -> None:
    """Run Loop the loop example workflow against a live server."""
    connection = await Connection.connect()
    worker = Worker(workflows=[ltl], connection=connection)
    worker_task = asyncio.create_task(worker.run())
    client = Client(connection=connection)

    try:
        await worker.wait_until_ready()

        workflow_id = str(ulid.ULID())
        name = "Integration Tester"

        result = await client.run_workflow(
            type=ltl.workflow_type,
            id=workflow_id,
            input={"start": 0},
            timeout=timedelta(seconds=30),
        )

        assert result == 1000
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        Connection.reset()
