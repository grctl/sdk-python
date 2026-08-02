import asyncio
from typing import Protocol

from grctl.exec.codec import Codec as ExecutionCodec
from grctl.exec.execution import DirectiveAPI, Execution, ExecutionDeps
from grctl.exec.kv_manager import Caster, KVApi, KVManager
from grctl.exec.step_history import HistoryWriter, StepHistory
from grctl.logging_config import get_logger
from grctl.models import Directive, HistoryEvent, RunInfo, Step
from grctl.models.command import WorkflowTypeDef
from grctl.models.errors import WorkflowStepAlreadyExecutedError
from grctl.models.worker import WorkerInfo
from grctl.workflow import Workflow
from grctl.workflow.future import HistoryListenerFactory
from grctl.workflow.handle import WorkflowAPI

logger = get_logger(__name__)


class HistoryReader(Protocol):
    """Durable store ExecutionManager reads a step's replay prefix from."""

    async def fetch_step_history(self, wf_id: str, run_id: str, history_seq_id: int) -> list[HistoryEvent]: ...


class Codec(ExecutionCodec, Caster, Protocol):
    """Connection.codec doubles as ExecutionDeps.codec and KVManager's caster."""


class Connection(Protocol):
    """What ExecutionManager needs from a connection to build an Execution's deps.

    A structural protocol rather than an import from nats/: exec/ stays free
    of any transport dependency, and any backend (NATS, an in-memory fake for
    tests, ...) can satisfy this without inheriting from it.
    """

    @property
    def history_reader(self) -> HistoryReader: ...

    @property
    def history_writer(self) -> HistoryWriter: ...

    @property
    def workflow_api(self) -> WorkflowAPI: ...

    @property
    def listener_factory(self) -> HistoryListenerFactory: ...

    @property
    def codec(self) -> Codec: ...

    def build_kv_api(self, run_info: RunInfo) -> KVApi: ...

    def build_directive_api(self, run_info: RunInfo) -> DirectiveAPI: ...


class WorkflowRegistry:
    """The single access point for the workflows this worker serves.

    Everything that needs a registered workflow (or its configs) goes through
    here rather than touching the workflow list directly.
    """

    def __init__(self, workflows: list[Workflow]) -> None:
        self.workflows = {wf.workflow_type: wf for wf in workflows}

    def all(self) -> list[Workflow]:
        return list(self.workflows.values())

    def types(self) -> list[str]:
        return list(self.workflows)

    def type_defs(self) -> list[WorkflowTypeDef]:
        """Structural definition of every registered workflow, for server registration."""
        return [wf.type_def() for wf in self.workflows.values()]

    def get(self, workflow_type: str) -> Workflow:
        workflow = self.workflows.get(workflow_type)
        if workflow is None:
            raise ValueError(
                f"No workflow registered for type '{workflow_type}'. Registered types: {list(self.workflows)}"
            )
        return workflow


class ExecutionManager:
    """Tracks in-flight step executions and builds new ones from directives.

    At most one execution runs per run_id at a time — a directive for a run_id
    that is already executing is rejected so the caller (Subscriber) can ACK it
    as a redelivery rather than double-run the step.
    """

    def __init__(self, registry: WorkflowRegistry, worker_info: WorkerInfo, connection: Connection) -> None:
        self.registry = registry
        self.worker_info = worker_info
        self.connection = connection
        self.executions: dict[str, asyncio.Task] = {}

    def is_running(self, run_id: str) -> bool:
        return run_id in self.executions

    def running_count(self) -> int:
        return len(self.executions)

    async def handle_next_directive(self, directive: Directive) -> asyncio.Task:
        """Build and start tracking the execution for a directive.

        Raises WorkflowStepAlreadyExecutedError if run_id is already executing —
        caller should ACK the message to deduplicate rather than retry.
        """
        run_id = directive.run_info.id
        if self.is_running(run_id):
            logger.warning("Run %s is already executing, rejecting duplicate directive", run_id)
            raise WorkflowStepAlreadyExecutedError(f"Workflow run {run_id} is already executing")

        execution = await self.build_execution(directive)
        task = asyncio.create_task(self.run(execution))
        self.executions[run_id] = task
        return task

    async def run(self, execution: Execution) -> None:
        run_id = execution.run_info.id
        try:
            await execution.create_task()
            await execution.task
        finally:
            self.executions.pop(run_id, None)

    def terminate(self, run_id: str) -> bool:
        """Cancel an in-flight execution. Returns True if one was found and cancelled."""
        task = self.executions.get(run_id)
        if task is None:
            return False
        logger.debug("Terminating execution for run_id=%s", run_id)
        task.cancel()
        return True

    async def shutdown(self) -> None:
        """Wait for all in-flight executions to complete."""
        if self.executions:
            logger.debug("Waiting for %d in-flight execution(s) to complete", len(self.executions))
            await asyncio.gather(*self.executions.values(), return_exceptions=True)
            self.executions.clear()

    async def build_execution(self, directive: Directive) -> Execution:
        # Only Step directives are supported so far — Start/Event/Cancel handling needs
        # exec/workflow realignment (see project/notes/sdk-rewrite.md) before this can
        # resolve a handler_config for them.
        if not isinstance(directive.msg, Step):
            raise NotImplementedError(f"Execution construction for directive kind '{directive.kind}' not supported")

        run_info = directive.run_info
        workflow = self.registry.get(run_info.wf_type)
        handler_config = workflow.step_handler(directive.msg.step_name)
        deps = await self.build_deps(directive)

        return Execution(
            worker_info=self.worker_info,
            directive=directive,
            handler_config=handler_config,
            deps=deps,
            logger=logger,
        )

    async def build_deps(self, directive: Directive) -> ExecutionDeps:
        """Wrap the connection's raw collaborators into the domain objects an Execution needs."""
        run_info = directive.run_info

        history_appender = StepHistory(
            writer=self.connection.history_writer,
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            worker_id=self.worker_info.id,
        )
        kvman = KVManager(
            kv_api=self.connection.build_kv_api(run_info),
            caster=self.connection.codec,
        )

        return ExecutionDeps(
            kvman=kvman,
            history_appender=history_appender,
            directive_api=self.connection.build_directive_api(run_info),
            codec=self.connection.codec,
            workflow_api=self.connection.workflow_api,
            listener_factory=self.connection.listener_factory,
            step_history=await self.load_step_history(directive),
        )

    async def load_step_history(self, directive: Directive) -> list[HistoryEvent]:
        """Fetch replay history for this step, or [] for a fresh (non-retry) attempt."""
        if directive.attempt <= 0:
            return []

        history_seq_id = directive.run_info.history_seq_id
        if history_seq_id <= 0:
            return []

        return await self.connection.history_reader.fetch_step_history(
            wf_id=directive.run_info.wf_id,
            run_id=directive.run_info.id,
            history_seq_id=history_seq_id,
        )
