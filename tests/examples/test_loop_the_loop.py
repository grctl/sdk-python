import asyncio
import contextlib
from datetime import timedelta

import pytest
import ulid

from examples.loop_the_loop import ltl
from grctl.client.client import Client
from grctl.nats.connection import Connection
from grctl.worker.worker import Worker


async def _wait_for_worker_ready(worker: Worker, worker_task: asyncio.Task) -> None:
    """Wait until worker has subscribed to workflow directives."""
    async with asyncio.timeout(5.0):
        while True:
            if worker._subscriber is not None:
                return
            if worker_task.done():
                worker_task.result()  # Propagate exception if task exited early
            await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_ltl_example() -> None:
    """Run Loop the loop example workflow against a live server."""
    connection = await Connection.connect()
    worker = Worker(workflows=[ltl], connection=connection)
    worker_task = asyncio.create_task(worker.start())
    client = Client(connection=connection)

    await asyncio.sleep(0.05)  # Give worker a moment to start

    try:
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
