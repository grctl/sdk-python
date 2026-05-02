import asyncio
import contextlib
from datetime import timedelta

import pytest
import ulid

from examples.wait_events import greet_events
from grctl.client.client import Client
from grctl.nats.connection import Connection
from grctl.worker.worker import Worker


@pytest.mark.asyncio
async def test_wait_events_example_end_to_end() -> None:
    """Run wait_events example workflow against a live server."""
    connection = await Connection.connect()
    worker = Worker(workflows=[greet_events], connection=connection)
    worker_task = asyncio.create_task(worker.start())
    client = Client(connection=connection)

    await asyncio.sleep(0.05)

    try:
        workflow_id = str(ulid.ULID())
        name = "World"

        wf_handle = await client.start_workflow(
            type=greet_events.workflow_type,
            id=workflow_id,
            input={"name": name},
            timeout=timedelta(seconds=30),
        )

        await asyncio.sleep(1)
        await wf_handle.send("greet", {"title": "Mr"})

        await asyncio.sleep(1)
        await wf_handle.send("farewell", {"farewell_note": "Until next time!"})

        result = await asyncio.wait_for(wf_handle.future, timeout=30)

        assert result == f"Hello, Mr {name}! Goodbye, {name}! Until next time!"
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        Connection.reset()


@pytest.mark.asyncio
async def test_get_workflow_handle_cross_process() -> None:
    """Process 1 starts the workflow; process 2 obtains a handle via get_workflow_handle and drives it to completion."""
    # --- Process 1: start the workflow ---
    connection_1 = await Connection.connect()
    worker = Worker(workflows=[greet_events], connection=connection_1)
    worker_task = asyncio.create_task(worker.start())
    client_1 = Client(connection=connection_1)

    await asyncio.sleep(0.05)

    workflow_id = str(ulid.ULID())
    name = "World"

    wf_handle = await client_1.start_workflow(
        type=greet_events.workflow_type,
        id=workflow_id,
        input={"name": name},
        timeout=timedelta(seconds=30),
    )

    # Wait for the workflow to reach its first wait_for_event state.
    await asyncio.sleep(1)

    try:
        # --- Process 2: independent connection, no knowledge of the original handle ---
        Connection.reset()
        connection_2 = await Connection.connect()
        client_2 = Client(connection=connection_2)

        handle_2 = await client_2.get_workflow_handle(workflow_id)
        await handle_2.send("greet", {"title": "Mr"})

        await asyncio.sleep(1)
        await handle_2.send("farewell", {"farewell_note": "Until next time!"})

        result = await asyncio.wait_for(handle_2.future, timeout=30)

        assert result == f"Hello, Mr {name}! Goodbye, {name}! Until next time!"
        # The original handle's future should also resolve since it shares the same history subject.
        assert await asyncio.wait_for(wf_handle.future, timeout=5) == result
    finally:
        await worker.stop()
        if not worker_task.done():
            worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        Connection.reset()
