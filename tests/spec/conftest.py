"""Spec test infrastructure.

Assumes grctld is already running. Start it before running spec tests:

    cd grctl && mise run start

Per-test NATS connection, grctl client, and worker factory fixtures.
"""

import asyncio
import contextlib
import os

import pytest

from grctl.client import Client, Connection
from grctl.worker import Worker
from grctl.workflow.workflow import Workflow

SPEC_NATS_URL = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")


@pytest.fixture
async def nats_connection():
    conn = await Connection.connect(servers=[SPEC_NATS_URL])
    yield conn
    try:
        with contextlib.suppress(Exception):
            await conn.close()
    finally:
        Connection.reset()


@pytest.fixture
async def grctl_client(nats_connection):
    return Client(connection=nats_connection)


@pytest.fixture
async def worker(nats_connection):
    """Start a worker with the given workflow definitions.

    Usage::

        async def test_something(worker, grctl_client):
            await worker([my_workflow])
            result = await grctl_client.run_workflow(...)
    """
    started: Worker | None = None
    worker_task: asyncio.Task[None] | None = None

    async def start(workflows: list[Workflow]) -> Worker:
        nonlocal started, worker_task
        w = Worker(workflows=workflows, connection=nats_connection)
        worker_task = asyncio.create_task(w.start())
        started = w

        await w.wait_until_ready()

        return w

    yield start

    if started is not None:
        with contextlib.suppress(Exception):
            await started.stop()
    if worker_task is not None:
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
