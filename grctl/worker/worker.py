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
from grctl.worker.manager import Connection, WorkerManager
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
        name: str | None = None,
    ) -> None:
        """Initialize the worker."""
        self._workflows = workflows
        self._connection = connection
        self._name = name
        self._stop_event = asyncio.Event()
        self._startup_event = asyncio.Event()
        self._manager: WorkerManager | None = None
        self._startup_error: Exception | None = None

    @cached_property
    def worker_name(self) -> str:
        """Human-readable name for this worker instance.

        Uses the explicit name if provided, otherwise derives a stable identifier
        from MD5 of sorted workflow type names.
        """
        if self._name:
            return self._name
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

        Registers the workflow catalog and subscribes to workflow subjects.
        """
        self._startup_event.clear()
        self._startup_error = None

        logger.info(
            f"Starting worker with {len(self._workflows)} registered workflows",
        )

        try:
            self._manager = WorkerManager(self._workflows, self._connection, self.worker_id, self.worker_name)
            await self._manager.start()
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
        if self._manager is None:
            raise RuntimeError("Worker startup completed without creating a manager")

    async def stop(self, shutdown_timeout: float = 30.0) -> None:
        """Stop the worker gracefully.

        Shutdown sequence:
        1. Stop accepting new messages
        2. Wait for in-flight workflows to complete (with timeout)

        Args:
            shutdown_timeout: Max seconds to wait for in-flight workflows

        """
        logger.info("Stopping worker - initiating graceful shutdown...")

        if self._manager is not None:
            await self._manager.stop(shutdown_timeout)

        self._stop_event.set()

        logger.info("Worker stopped gracefully")
