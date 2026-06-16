"""Worker implementation for processing workflow tasks.

Workers consume messages from NATS streams and execute workflows.
They support horizontal scaling through queue groups.
"""

import asyncio
import hashlib
import secrets
import socket
from functools import cached_property

from grctl.logging_config import get_logger
from grctl.nats.connection import Connection
from grctl.nats.wf_subscriber import Subscriber
from grctl.worker.registration import build_catalog, register_workflow_types
from grctl.worker.run_manager import RunManager
from grctl.worker.worker_cmd_subscriber import WorkerCmdSubscriber
from grctl.workflow.workflow import Workflow

logger = get_logger(__name__)


class Worker:
    """Worker that processes workflow messages.

    Workers are initialized with a list of workflow instances and subscribe
    to their corresponding NATS subjects using queue groups for load balancing.

    Example:
        order_wf = Workflow(name="order_wf")
        payment_wf = Workflow(name="payment_wf")

        connection = await Connection.connect()
        worker = Worker(
            workflows=[order_wf, payment_wf],
            connection=connection,
        )
        await worker.start()

    """

    def __init__(
        self,
        workflows: list[Workflow],
        connection: Connection,
    ) -> None:
        """Initialize the worker."""
        self._workflows = workflows
        self._connection = connection
        self._stop_event = asyncio.Event()
        self._startup_event = asyncio.Event()
        self._subscriber: Subscriber | None = None
        self._worker_cmd_subscriber: WorkerCmdSubscriber | None = None
        self._run_manager: RunManager | None = None
        self._startup_error: Exception | None = None

    @cached_property
    def worker_name(self) -> str:
        """Stable identifier shared across all instances with the same workflow set.

        Derived from MD5 of sorted workflow type names — identical across processes
        with the same registered workflows.
        """
        workflow_types = sorted([wf.workflow_type for wf in self._workflows])
        types_str = "|".join(workflow_types)
        hash_digest = hashlib.md5(types_str.encode()).hexdigest()
        return hash_digest[:5]

    @cached_property
    def worker_id(self) -> str:
        """Unique per-instance identifier combining stable name hash, random suffix, and hostname."""
        random_chars = secrets.token_hex(1)
        hostname = socket.gethostname()
        return f"w_{self.worker_name}.{random_chars}@{hostname}"

    async def run(self) -> None:
        """Run the worker and begin processing messages.

        Creates RunManager for workflow execution and subscribes to workflow subjects.
        """
        self._startup_event.clear()
        self._startup_error = None

        logger.info(
            f"Starting worker with {len(self._workflows)} registered workflows",
        )

        try:
            await self._setup()
            self._startup_event.set()
            logger.info(f"Worker {self.worker_name} ({self.worker_id}) started and ready to process messages")

            # Keep worker alive
            await self._stop_event.wait()
        except Exception as exc:
            self._startup_error = exc
            self._startup_event.set()
            raise

    async def wait_until_ready(self, timeout_ms: float = 5.0) -> None:
        """Wait until worker startup succeeds or fails."""
        await asyncio.wait_for(self._startup_event.wait(), timeout=timeout_ms)
        if self._startup_error is not None:
            raise self._startup_error
        if self._subscriber is None:
            raise RuntimeError("Worker startup completed without creating a subscriber")

    async def _setup(self) -> None:
        self._run_manager = RunManager(
            worker_name=self.worker_name,
            worker_id=self.worker_id,
            workflows=self._workflows,
            connection=self._connection,
        )

        # Register workflow types with the server before claiming any work.
        # A failure here raises and aborts startup — fail fast, loud.
        catalog = build_catalog(self._workflows)
        await register_workflow_types(self._connection, self.worker_id, catalog)

        wf_types = [wf.workflow_type for wf in self._workflows]
        self._subscriber = Subscriber(
            js=self._connection.jetstream,
            manifest=self._connection.manifest,
            wf_types=wf_types,
            run_manager=self._run_manager,
            logger=logger,
        )
        await self._subscriber.start()

        self._worker_cmd_subscriber = WorkerCmdSubscriber(
            nc=self._connection.nc,
            manifest=self._connection.manifest,
            worker_id=self.worker_id,
            run_manager=self._run_manager,
        )
        await self._worker_cmd_subscriber.start()

    async def stop(self, shutdown_timeout: float = 30.0) -> None:
        """Stop the worker gracefully.

        Shutdown sequence:
        1. Stop accepting new messages
        2. Wait for in-flight workflows to complete (with timeout)
        3. Close NATS connection

        Args:
            shutdown_timeout: Max seconds to wait for in-flight workflows

        """
        logger.info("Stopping worker - initiating graceful shutdown...")

        # 1. Stop accepting new messages
        if self._worker_cmd_subscriber is not None:
            await self._worker_cmd_subscriber.stop()
            self._worker_cmd_subscriber = None

        if self._subscriber is not None:
            logger.info("Stopping subscriber (no new messages will be accepted)")
            await self._subscriber.stop()

        # 2. Wait for in-flight workflows with timeout
        if self._run_manager:
            running_count = self._run_manager.get_running_count()
            if running_count > 0:
                logger.info(f"Waiting for {running_count} in-flight workflows (timeout: {shutdown_timeout}s)")
                try:
                    await asyncio.wait_for(self._run_manager.shutdown(), timeout=shutdown_timeout)
                    logger.info("All in-flight workflows completed successfully")
                except TimeoutError:
                    logger.warning(
                        f"Shutdown timeout after {shutdown_timeout}s - "
                        f"terminating {self._run_manager.get_running_count()} remaining workflows"
                    )

        # 3. Close NATS connection
        await self._connection.close()

        # 4. Signal stop event (releases _process_messages)
        self._stop_event.set()

        logger.info("Worker stopped gracefully")
