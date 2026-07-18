import asyncio
import json
from typing import TYPE_CHECKING

from grctl.logging_config import get_logger
from grctl.models import HistoryEvent, RunInfo, Step
from grctl.models.directive import Directive
from grctl.nats.history_fetch import fetch_step_history
from grctl.nats.kv_store import KVStore as NatsKVStore
from grctl.nats.wf_subscriber import WorkflowStepAlreadyExecutedError
from grctl.worker import Context
from grctl.worker.kv_store import KVStore
from grctl.worker.runner import WorkflowRunner
from grctl.worker.runtime import StepRuntime
from grctl.workflow.workflow import Workflow

if TYPE_CHECKING:
    from grctl.nats.connection import Connection

logger = get_logger(__name__)


class RunManager:
    """Manages lifecycle of workflow runner tasks.

    Tracks running workflows and ensures only one task per run_id executes at a time.
    Automatically cleans up completed tasks.
    """

    def __init__(
        self,
        worker_name: str,
        worker_id: str,
        workflows: list[Workflow],
        connection: "Connection",
    ) -> None:
        self.worker_name = worker_name
        self.worker_id = worker_id
        self.workflows = {wf.workflow_type: wf for wf in workflows}
        self.connection = connection
        self.runner_tasks: dict[str, asyncio.Task] = {}
        self.runner_directives: dict[str, Directive] = {}

    def is_running(self, run_id: str) -> bool:
        """Check if a workflow run is currently executing."""
        return run_id in self.runner_tasks

    def get_worker_name(self) -> str:
        return self.worker_name

    def get_workflow_types(self) -> list[str]:
        """Get list of workflow types managed by this RunManager."""
        return list(self.workflows.keys())

    async def handle_next_directive(self, directive: Directive) -> asyncio.Task:
        """Initialize and start directive handling.

        Raises ValueError if the workflow type is not registered — caller should NAK the message.
        Returns the running asyncio.Task, or None if the run_id is already executing (caller
        should ACK to deduplicate).
        """
        workflow = self.workflows.get(directive.run_info.wf_type)
        if workflow is None:
            raise ValueError(
                f"No workflow registered for type '{directive.run_info.wf_type}'. "
                f"Registered types: {list(self.workflows.keys())}"
            )

        step_history = await self._load_step_history(directive)

        runtime = StepRuntime(
            workflow=workflow,
            worker_id=self.worker_id,
            directive=directive,
            connection=self.connection,
            step_history=step_history,
        )

        context = Context(
            run_info=directive.run_info,
            kv_store=self._create_kv_store(directive.run_info),
            worker_id=self.worker_id,
            directive=directive,
            parent_run=self._create_parent_run(directive.run_info),
            step_configs=workflow._step_handlers,  # noqa: SLF001
            runtime=runtime,
        )

        runner = WorkflowRunner(runtime, context)

        task = self._start_task(runner, directive)
        await self._publish_metrics()
        return task

    def _start_task(self, runner: WorkflowRunner, directive: Directive) -> asyncio.Task:
        """Start a tracked asyncio task for the runner.

        Returns None if the run_id is already executing (duplicate message).
        """
        run_id = runner.runtime.run_info.id

        if self.is_running(run_id):
            step_name = directive.msg.step_name if isinstance(directive.msg, Step) else None
            logger.warning(
                "Workflow run %s is already executing, skipping attempt:%s, step_name: %s",
                run_id,
                directive.attempt,
                step_name,
            )
            current_directive = self.runner_directives.get(run_id)
            if current_directive is not None:
                step_name = current_directive.msg.step_name if isinstance(current_directive.msg, Step) else None
                logger.warning(
                    "Current running run_id: %s, attempt: %s step_name: %s",
                    run_id,
                    current_directive.attempt,
                    step_name,
                )
            else:
                logger.warning("No current directive found for run_id: %s", run_id)
            raise WorkflowStepAlreadyExecutedError(f"Workflow run {run_id} is already executing")

        task = asyncio.create_task(self._run_with_cleanup(runner, directive))
        self.runner_tasks[run_id] = task
        self.runner_directives[run_id] = directive

        step_name = directive.msg.step_name if isinstance(directive.msg, Step) else None
        logger.debug(
            "Started workflow runner task for run_id: %s, step_name: %s, attempt: %s",
            run_id,
            step_name,
            directive.attempt,
        )
        return task

    async def _load_step_history(self, directive: Directive) -> list[HistoryEvent]:
        if directive.attempt <= 0:
            return []

        # As a hard rule, run_info.history_seq_id always starts after the previous step completion
        history_seq_id = directive.run_info.history_seq_id
        if history_seq_id <= 0:
            return []

        return await fetch_step_history(
            js=self.connection.js,
            manifest=self.connection.manifest,
            wf_id=directive.run_info.wf_id,
            run_id=directive.run_info.id,
            history_seq_id=history_seq_id,
        )

    async def _run_with_cleanup(self, runner: WorkflowRunner, directive: Directive) -> None:
        """Execute runner and cleanup on completion."""
        run_id = runner.runtime.run_info.id
        try:
            await runner.handle_directive(directive)
        finally:
            self.runner_tasks.pop(run_id, None)
            self.runner_directives.pop(run_id, None)
            await self._publish_metrics()
            step_name = directive.msg.step_name if isinstance(directive.msg, Step) else None
            logger.debug(
                "Cleaned up runner job for run_id: %s directive_id: %s, directive kind: %s, step_name: %s",
                run_id,
                directive.id,
                directive.kind,
                step_name,
            )

    def _create_parent_run(self, run_info: RunInfo) -> RunInfo | None:
        if run_info.parent_run_id and run_info.parent_wf_id:
            return RunInfo(
                id=run_info.parent_run_id,
                wf_id=run_info.parent_wf_id,
                wf_type=run_info.parent_wf_type or "",
            )
        return None

    def get_runner_task(self, run_id: str) -> asyncio.Task | None:
        """Return the in-flight task for a run, or None if not running."""
        return self.runner_tasks.get(run_id)

    def terminate_run(self, run_id: str) -> bool:
        """Cancel an in-flight run job. Returns True if the task was found and cancelled."""
        task = self.runner_tasks.get(run_id)
        if task is None:
            return False

        logger.debug(f"Terminating worker job for run_id={run_id}")
        task.cancel()
        return True

    async def shutdown(self) -> None:
        """Wait for all running tasks to complete."""
        if self.runner_tasks:
            logger.debug(f"Waiting for {len(self.runner_tasks)} runner tasks to complete")
            await asyncio.gather(*self.runner_tasks.values(), return_exceptions=True)
            self.runner_tasks.clear()

    def get_running_count(self) -> int:
        """Get number of currently running workflow tasks."""
        return len(self.runner_tasks)

    async def _publish_metrics(self) -> None:
        payload = json.dumps(
            {
                "worker_name": self.worker_name,
                "active_steps": len(self.runner_tasks),
            }
        ).encode()
        subject = f"grctl.worker.{self.worker_id}.metrics"
        try:
            await self.connection.nc.publish(subject, payload)
        except Exception:
            logger.debug("Failed to publish worker metrics")

    def _create_kv_store(self, run_info: RunInfo) -> KVStore:
        kv_store = NatsKVStore(self.connection.js, self.connection.manifest, run_info)
        return KVStore(loader=kv_store.load, codec=self.connection.codec)
