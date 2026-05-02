import asyncio
import contextlib
import logging
from datetime import timedelta

import pytest
import ulid

from examples.child_workflow import order_wf, payment_wf
from grctl.client.client import Client
from grctl.nats.connection import Connection
from grctl.worker.worker import Worker

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_child_workflow_example_end_to_end() -> None:
    """Run child_workflow example workflow against a live server."""
    connection = await Connection.connect()
    worker = Worker(workflows=[order_wf, payment_wf], connection=connection, workflow_logger=logger)
    worker_task = asyncio.create_task(worker.start())
    client = Client(connection=connection)

    await asyncio.sleep(0.05)

    try:
        workflow_id = str(ulid.ULID())

        order_handle = await client.start_workflow(
            type=order_wf.workflow_type,
            id=workflow_id,
            input={"order_id": "ORDER-001", "amount": 99.99},
            timeout=timedelta(seconds=30),
        )

        await asyncio.sleep(2)
        await order_handle.send("send_to_payment")

        result = await asyncio.wait_for(order_handle.future, timeout=30)

        assert "ORDER-001" in result
        assert "success" in result
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        Connection.reset()
