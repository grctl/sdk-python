import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

from grctl.exec.manager import Connection as ExecConnection
from grctl.exec.manager import ExecutionManager
from grctl.logging_config import get_logger
from grctl.models import Command, Directive, GrctlAPIResponse, WorkflowTypeDef
from grctl.models.worker import WorkerInfo
from grctl.worker.cmd_handler import CMDHandler
from grctl.worker.errors import RegistrationError
from grctl.worker.registry import WorkflowRegistry
from grctl.workflow.workflow import Workflow

logger = get_logger(__name__)


class Listener(Protocol):
    """What WorkerManager needs from a subscription it owns the lifecycle of."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class WorkerAPI(Protocol):
    """Worker-scoped calls to the server that WorkerManager makes: register this worker."""

    async def register_worker(self, worker_id: str, catalog: list[WorkflowTypeDef]) -> GrctlAPIResponse: ...


DirectiveHandler = Callable[[Directive], Awaitable[asyncio.Task]]


class Connection(ExecConnection, Protocol):
    """What WorkerManager needs from a connection.

    Includes what it hands to ExecutionManager plus the server API, since both
    come from this same object — WorkerManager is the composition root holding
    the one reference.
    """

    def build_worker_cmd_listener(self, worker_id: str, handler: Callable[[Command], Awaitable[bool]]) -> Listener: ...

    def build_exec_job_listener(
        self, wf_types: list[str], directive_handler: DirectiveHandler, logger: logging.Logger
    ) -> Listener: ...

    @property
    def worker_api(self) -> WorkerAPI: ...


class WorkerManager:
    """Owns everything a Worker needs beyond its public run/stop contract."""

    def __init__(
        self,
        workflows: list[Workflow],
        connection: Connection,
        worker_id: str,
        worker_name: str,
    ) -> None:
        self.connection = connection
        self.worker_id = worker_id
        self.worker_info = WorkerInfo(id=worker_id, name=worker_name)
        self.registry = WorkflowRegistry(workflows)
        self.execution_manager = ExecutionManager(self.registry.get, self.worker_info, connection)
        self.exec_job_listener: Listener
        self.worker_cmd_listener: Listener

    async def start(self) -> None:
        # Register this worker with the server before claiming any work.
        # A failure here raises and aborts startup — fail fast, loud.
        response = await self.connection.worker_api.register_worker(self.worker_id, self.registry.type_defs())
        if not response.success:
            raise RegistrationError(f"server rejected registration: {response.error}")

        self.exec_job_listener = self.connection.build_exec_job_listener(
            self.registry.types(), self.execution_manager.handle_next_directive, logger
        )
        await self.exec_job_listener.start()

        worker_command_handler = CMDHandler(self.execution_manager)
        self.worker_cmd_listener = self.connection.build_worker_cmd_listener(
            self.worker_id, worker_command_handler.handle
        )
        await self.worker_cmd_listener.start()

    async def stop(self, shutdown_timeout: float = 30.0) -> None:
        """Stop accepting new work, then wait for in-flight executions (with timeout)."""
        if self.worker_cmd_listener is not None:
            await self.worker_cmd_listener.stop()

        if self.exec_job_listener is not None:
            logger.info("Stopping subscriber (no new messages will be accepted)")
            await self.exec_job_listener.stop()

        running_count = self.execution_manager.running_count()
        if running_count > 0:
            logger.info("Waiting for %d in-flight execution(s) (timeout: %ss)", running_count, shutdown_timeout)
            try:
                await asyncio.wait_for(self.execution_manager.shutdown(), timeout=shutdown_timeout)
                logger.info("All in-flight executions completed successfully")
            except TimeoutError:
                logger.warning(
                    "Shutdown timeout after %ss - terminating %d remaining execution(s)",
                    shutdown_timeout,
                    self.execution_manager.running_count(),
                )
