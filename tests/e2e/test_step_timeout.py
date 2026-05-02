import asyncio
import multiprocessing
from datetime import timedelta

import pytest
import ulid

from grctl.client.client import Client
from grctl.logging_config import get_logger, setup_logging
from grctl.models import Directive
from grctl.models.errors import WorkflowError
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.worker import Worker
from grctl.workflow import Workflow

setup_logging()
logger = get_logger(__name__)


def _worker_process_main(timeout_seconds: float = 60.0) -> None:
    wf = Workflow(workflow_type="StepTimeoutWorkflow")

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(slow_step)

    @wf.step(timeout=timedelta(seconds=2))
    async def slow_step(ctx: Context) -> Directive:
        await asyncio.sleep(30)
        return ctx.next.complete("should not reach here")

    async def run_worker() -> None:
        connection = await Connection.connect()
        worker = Worker(workflows=[wf], connection=connection)
        try:
            await asyncio.wait_for(worker.start(), timeout=timeout_seconds)
        except TimeoutError:
            pass
        except Exception:
            logger.exception("Worker error")

    asyncio.run(run_worker())


async def test_step_timeout() -> None:
    """Step with a 2s timeout that sleeps 30s should fail."""
    connection = await Connection.connect()
    client = Client(connection=connection)

    worker_process = multiprocessing.Process(
        target=_worker_process_main,
        daemon=True,
    )
    worker_process.start()
    await asyncio.sleep(0.5)

    try:
        workflow_id = str(ulid.ULID())

        with pytest.raises(WorkflowError) as exc_info:
            await client.run_workflow(
                type="StepTimeoutWorkflow",
                id=workflow_id,
                input={},
                timeout=timedelta(seconds=30),
            )

        error_message = str(exc_info.value)
        logger.info(f"Caught expected timeout error: {error_message}")
        assert "TimeoutError" in error_message or "timed out" in error_message.lower(), (
            f"Expected timeout error, got: {error_message}"
        )

    finally:
        if worker_process.is_alive():
            worker_process.terminate()
            worker_process.join(timeout=5.0)
            if worker_process.is_alive():
                worker_process.kill()
                worker_process.join(timeout=1.0)
        Connection.reset()
