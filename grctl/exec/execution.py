import asyncio
import contextlib
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from logging import Logger
from typing import Any, Protocol

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.codec import Codec
from grctl.exec.context import Context
from grctl.exec.drc_factory import DrcFactory
from grctl.exec.kv_manager import KVManager
from grctl.exec.operation_history import OperationHistory, StepHistoryAppender
from grctl.exec.task import reset_current_context, set_current_context
from grctl.exec.workflow_logger import build_workflow_logger
from grctl.models import Directive, ErrorDetails, HistoryEvent, RunInfo, Step
from grctl.models.directive import NextMessage
from grctl.models.handler import RegisteredStep
from grctl.models.worker import WorkerInfo
from grctl.workflow.handle import WorkflowAPI, WorkflowHandleFactory
from grctl.workflow.workflow import StepInfo


class DirectiveAPI(Protocol):
    """Outbound port an Execution publishes its step directives through."""

    async def send(self, directive: Directive) -> None: ...


@dataclass
class ExecutionDeps:
    """Everything an Execution needs from the outside world, for one directive."""

    kvman: KVManager
    history_appender: StepHistoryAppender
    directive_api: DirectiveAPI
    codec: Codec
    workflow_api: WorkflowAPI
    handle_factory: WorkflowHandleFactory
    step_history: list[HistoryEvent]
    step_infos: Mapping[str, StepInfo]


class Execution:
    run_info: RunInfo
    worker_info: WorkerInfo
    directive: Directive
    handler_config: RegisteredStep

    # Child handles started during this step. They are single-step-scoped: cross-step
    # coordination uses events/callbacks, not in-memory futures, so any handle still
    # open when the step returns is abandoned and gets discarded.
    childs: ChildTracker

    # When this attempt at the step began. A re-delivered step gets a fresh one: the
    # duration reported is this attempt's, not the wall time since the step first ran.
    started_at: datetime

    def __init__(
        self,
        worker_info: WorkerInfo,
        directive: Directive,
        handler_config: RegisteredStep,
        deps: ExecutionDeps,
        logger: Logger,
    ) -> None:
        self.deps = deps
        self.run_info = directive.run_info
        self.worker_info = worker_info
        self.directive = directive
        self.handler_config = handler_config
        self.started_at = datetime.now(UTC)
        self.kvman = deps.kvman
        self.directive_api = deps.directive_api
        self.codec = deps.codec
        self.logger = logger
        self.step_directive_factory = DrcFactory(self.run_info, worker_info.id, directive, deps.step_infos)

        # Async task for step execution
        self.task: asyncio.Task

        self.is_executing = False

    async def create_task(self) -> None:
        self.task = asyncio.create_task(self.execute())

    async def execute(self) -> None:
        await self.send_step_picked_up()
        handler = self.handler_config.handler
        # Stays None if the step is cancelled: a step that never reached an outcome has
        # nothing to report, and what becomes of a run whose worker went away is the
        # server's decision, not ours.
        outcome_directive: Directive | None = None
        context_token = None
        childs: ChildTracker | None = None
        try:
            self.is_executing = True
            context, childs = self._build_context()
            context_token = set_current_context(context)
            payload = self.get_serialised_handler_payload()
            if payload is None:
                outcome_directive = await handler(context)
            else:
                outcome_directive = await handler(context, **payload)
        except Exception as e:
            stack_trace = traceback.format_exc()
            self.logger.exception(f"Workflow execution failed for {self.step_name}")
            outcome_directive = self.step_directive_factory.fail_step(
                self.step_name,
                ErrorDetails(type=type(e).__name__, message=str(e), stack_trace=stack_trace),
            )
        finally:
            if context_token is not None:
                reset_current_context(context_token)
            self.is_executing = False
            # Always release child handles started in this step, even when the handler
            # raised, so an unawaited future never warns or leaks its subscription.
            if childs is not None:
                await childs.discard_all()
            if outcome_directive is not None:
                await self.send_step_result(outcome_directive)

    def _build_context(self) -> tuple[Context, ChildTracker]:
        step_history = self.deps.step_history or []
        childs = ChildTracker()
        operation_history = OperationHistory(step_history, self.deps.history_appender)
        context = Context(
            operation_history,
            self.run_info,
            self.worker_info.id,
            self.step_name,
            self.step_directive_factory,
            self.kvman,
            self.deps.workflow_api,
            self.deps.handle_factory,
            childs,
            self.codec,
            build_workflow_logger(operation_history, self.run_info, self.worker_info.id, self.step_name),
            self.parent_run,
        )
        return context, childs

    async def terminate(self) -> None:
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task

    @property
    def drc_msg(self) -> Step:
        if isinstance(self.directive.msg, Step):
            return self.directive.msg

        raise ValueError("Only steps can be executed")

    @property
    def step_name(self) -> str:
        return self.drc_msg.step_name

    @property
    def payload(self) -> Any | None:
        return self.drc_msg.payload

    @property
    def parent_run(self) -> RunInfo | None:
        if self.run_info.parent_run_id and self.run_info.parent_wf_id:
            return RunInfo(
                id=self.run_info.parent_run_id,
                wf_id=self.run_info.parent_wf_id,
                wf_type=self.run_info.parent_wf_type or "",
            )
        return None

    async def send_step_picked_up(self) -> None:
        """Announce the step, once. A re-delivered step was already announced by the attempt that died."""
        if not self.deps.step_history:
            drc = self.step_directive_factory.step_picked_up(step_name=self.step_name, timestamp=self.started_at)
            await self.directive_api.send(drc)

    async def send_step_result(self, directive: Directive) -> None:
        if not isinstance(directive.msg, NextMessage):
            raise ValueError("Wrong message kind for step result")  # noqa: TRY004

        duration_ms = int((datetime.now(UTC) - self.started_at).total_seconds() * 1000)

        pending_updates = self.kvman.get_pending_updates()

        drc = self.step_directive_factory.step_result(
            next_msg_kind=directive.kind,
            next_msg=directive.msg,
            timestamp=datetime.now(UTC),
            kv_updates=pending_updates,
            duration_ms=duration_ms,
        )
        await self.directive_api.send(drc)

    def get_serialised_handler_payload(self) -> dict[str, Any] | None:
        spec = self.handler_config.spec
        if not spec.payload_parameters or self.payload is None:
            return None

        # Single param: if payload is already keyed by param name use the value,
        # otherwise treat payload itself as the value (e.g. bare Pydantic model).
        if len(spec.payload_parameters) == 1:
            name, param_type = next(iter(spec.payload_parameters.items()))
            raw = self.payload[name] if isinstance(self.payload, dict) and name in self.payload else self.payload
            typed_value = self.codec.from_primitive(raw, param_type)
            return {name: typed_value}

        # Multi param: convert each param from the payload dict and pass as kwargs
        if not isinstance(self.payload, dict):
            raise TypeError(
                "Handler expects payload parameters "
                f"{list(spec.payload_parameters)} but self.payload is not a dict: {type(self.payload)}"
            )

        return {
            name: self.codec.from_primitive(self.payload[name], param_type)
            for name, param_type in spec.payload_parameters.items()
            if name in self.payload
        }
