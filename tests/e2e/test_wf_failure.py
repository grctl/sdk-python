"""E2E test for step failure propagation to client.

This test verifies that when a workflow step fails, the failure
is properly propagated to the client through the NATS event system.

This version runs the worker in a separate process to better simulate
real-world deployment scenarios.
"""

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
from grctl.worker.task import task
from grctl.worker.worker import Worker
from grctl.workflow import Workflow

setup_logging()
logger = get_logger(__name__)

# Global flag for worker process
_worker_running = multiprocessing.Event()


def _worker_process_main(workflow_type: str, timeout_seconds: float = 60.0) -> None:
    """Run worker in a separate process.

    This function must be defined at module level to be picklable by multiprocessing.
    """
    # Create a simple workflow that fails
    failing_wf = Workflow(workflow_type=workflow_type)

    @task
    async def failing_task() -> str:
        """Raise an exception to simulate task failure."""
        logger = get_logger(__name__)
        logger.info("Executing failing task...")
        raise ValueError("Intentional task failure for testing")

    @failing_wf.start()
    async def failing_start(ctx: Context, name: str) -> Directive:
        """Start handler that calls a failing task."""
        logger = get_logger(__name__)
        logger.info(f"Starting failing workflow for: {name}")
        result = await failing_task()
        return ctx.next.complete(result)

    async def run_worker() -> None:
        connection = await Connection.connect()
        worker = Worker(workflows=[failing_wf], connection=connection)
        try:
            await asyncio.wait_for(worker.start(), timeout=timeout_seconds)
        except TimeoutError:
            pass
        except Exception:
            logger.exception("Worker error")

    asyncio.run(run_worker())


def _step_failure_worker_process_main(timeout_seconds: float = 60.0) -> None:
    """Worker process that fails directly in the step handler."""
    step_failing_wf = Workflow(workflow_type="StepFailingWorkflow")

    @step_failing_wf.start()
    async def step_failing_start(ctx: Context) -> Directive:
        logger = get_logger(__name__)
        logger.info("Starting step failing workflow...")
        raise RuntimeError("Direct step handler failure")

    async def run_worker() -> None:
        connection = await Connection.connect()
        worker = Worker(workflows=[step_failing_wf], connection=connection)
        try:
            await asyncio.wait_for(worker.start(), timeout=timeout_seconds)
        except TimeoutError:
            pass
        except Exception:
            logger.exception("Worker error")

    asyncio.run(run_worker())


@pytest.mark.asyncio
async def test_step_failure_propagated_to_client() -> None:
    """Test that step failures are properly propagated to the client.

    This test:
    1. Starts a worker in a separate process with a workflow that has a failing task
    2. Uses the client to run the workflow
    3. Verifies that the client receives the failure exception
    """
    connection = await Connection.connect()
    client = Client(connection=connection)
    workflow_type = "FailingWorkflow"

    # Start worker in a separate process
    worker_process = multiprocessing.Process(
        target=_worker_process_main,
        args=(workflow_type,),
        daemon=True,
    )
    worker_process.start()

    # Wait a moment for worker to initialize
    await asyncio.sleep(0.5)

    try:
        workflow_id = str(ulid.ULID())
        test_name = "Failure Test"

        # Expect the workflow to fail with a RuntimeError containing the original error
        with pytest.raises(WorkflowError) as exc_info:
            await client.run_workflow(
                type=workflow_type,
                id=workflow_id,
                input={"name": test_name},
                timeout=timedelta(seconds=30),
            )

        # Verify the error message contains information about the original failure
        error_message = str(exc_info.value)
        logger.info(f"Caught expected error: {error_message}")

        # The error should contain information about the ValueError
        assert "ValueError" in error_message or "Intentional task failure" in error_message, (
            f"Expected error message to contain 'ValueError' or 'Intentional task failure', but got: {error_message}"
        )

    finally:
        # Terminate the worker process
        if worker_process.is_alive():
            worker_process.terminate()
            worker_process.join(timeout=5.0)
            if worker_process.is_alive():
                worker_process.kill()
                worker_process.join(timeout=1.0)
        Connection.reset()


@pytest.mark.asyncio
async def test_step_failure_in_step_handler() -> None:
    """Test that failures in step handlers (not just tasks) are propagated.

    This test verifies that exceptions raised directly in step handlers
    are also properly propagated to the client.
    """
    connection = await Connection.connect()
    client = Client(connection=connection)

    # Start worker in a separate process
    worker_process = multiprocessing.Process(
        target=_step_failure_worker_process_main,
        daemon=True,
    )
    worker_process.start()

    # Wait a moment for worker to initialize
    await asyncio.sleep(0.5)

    try:
        workflow_id = str(ulid.ULID())

        with pytest.raises(WorkflowError) as exc_info:
            await client.run_workflow(
                type="StepFailingWorkflow",
                id=workflow_id,
                input={},
                timeout=timedelta(seconds=30),
            )

        error_message = str(exc_info.value)
        logger.info(f"Caught expected error from step handler: {error_message}")

        # The error should contain information about the RuntimeError
        assert "RuntimeError" in error_message or "Direct step handler failure" in error_message, (
            f"Expected error message to contain 'RuntimeError' or 'Direct step handler failure', "
            f"but got: {error_message}"
        )

    finally:
        # Terminate the worker process
        if worker_process.is_alive():
            worker_process.terminate()
            worker_process.join(timeout=5.0)
            if worker_process.is_alive():
                worker_process.kill()
                worker_process.join(timeout=1.0)
        Connection.reset()
