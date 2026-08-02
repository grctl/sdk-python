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
from grctl.exec.journal import Journal, StepHistoryAppender
from grctl.exec.kv_manager import KVManager
from grctl.exec.task import reset_current_context, set_current_context
from grctl.models import Directive, ErrorDetails, HistoryEvent, RunInfo, Step
from grctl.models.directive import NextMessage
from grctl.models.handler import HandlerConfig
from grctl.models.worker import WorkerInfo
from grctl.workflow.future import HistoryListenerFactory
from grctl.workflow.handle import WorkflowAPI
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
    listener_factory: HistoryListenerFactory
    step_history: list[HistoryEvent]
    step_infos: Mapping[str, StepInfo]


class Execution:
    run_info: RunInfo
    worker_info: WorkerInfo
    directive: Directive
    handler_config: HandlerConfig

    # Child handles started during this step. They are single-step-scoped: cross-step
    # coordination uses events/callbacks, not in-memory futures, so any handle still
    # open when the step returns is abandoned and gets discarded.
    childs: ChildTracker

    # The time at which this execution started.
    started_at: datetime

    def __init__(
        self,
        worker_info: WorkerInfo,
        directive: Directive,
        handler_config: HandlerConfig,
        deps: ExecutionDeps,
        logger: Logger,
    ) -> None:
        self.run_info = directive.run_info
        self.worker_info = worker_info
        self.directive = directive
        self.handler_config = handler_config
        self.childs = ChildTracker()
        self.kvman = deps.kvman
        self.step_history = deps.step_history or []
        self.history_appender = deps.history_appender
        self.directive_api = deps.directive_api
        self.codec = deps.codec
        self.logger = logger
        self.step_directive_factory = DrcFactory(self.run_info, worker_info.id, directive, deps.step_infos)
        self.journal = Journal(self.step_history, self.history_appender)
        self.context = Context(
            self.journal,
            self.run_info,
            worker_info.id,
            self.step_directive_factory,
            self.kvman,
            deps.workflow_api,
            deps.listener_factory,
            logger,
            self.childs,
            deps.codec,
            self.parent_run,
        )

        # Async task for step execution
        self.task: asyncio.Task

        self.is_executing = False

    async def create_task(self) -> None:
        self.task = asyncio.create_task(self.execute())

    async def execute(self) -> None:
        await self.send_step_picked_up()
        handler = self.handler_config.handler
        payload = self.get_serialised_handler_payload()
        outcome_directive: Directive
        context_token = None
        try:
            self.is_executing = True
            context_token = set_current_context(self.context)
            if payload is None:
                outcome_directive = await handler(self.context)
            else:
                outcome_directive = await handler(self.context, **payload)
        except Exception as e:
            stack_trace = traceback.format_exc()
            self.logger.exception(f"Workflow execution failed for {self.step_name}")
            outcome_directive = self.step_directive_factory.fail(
                ErrorDetails(type=type(e).__name__, message=str(e), stack_trace=stack_trace)
            )
        finally:
            if context_token is not None:
                reset_current_context(context_token)
            self.is_executing = False
            # Always release child handles started in this step, even when the handler
            # raised, so an unawaited future never warns or leaks its subscription.
            await self.childs.discard_all()
            await self.send_step_result(outcome_directive)

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
        if self.step_history is None or len(self.step_history) == 0:
            self.started_at = datetime.now(UTC)
            drc = self.step_directive_factory.step_picked_up(step_name=self.step_name, timestamp=self.started_at)
            await self.directive_api.send(drc)

    async def send_step_result(self, directive: Directive) -> None:
        if not isinstance(directive.msg, NextMessage):
            raise ValueError("Wrong message kind for step result")  # noqa: TRY004

        if self.started_at is not None:
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
        if not spec.params or self.payload is None:
            return None

        # Single param: if payload is already keyed by param name use the value,
        # otherwise treat payload itself as the value (e.g. bare Pydantic model).
        if len(spec.params) == 1:
            name, param_type = next(iter(spec.params.items()))
            raw = self.payload[name] if isinstance(self.payload, dict) and name in self.payload else self.payload
            typed_value = self.codec.from_primitive(raw, param_type)
            return {name: typed_value}

        # Multi param: convert each param from the payload dict and pass as kwargs
        if not isinstance(self.payload, dict):
            raise TypeError(
                f"Handler expects params {list(spec.params)} but self.payload is not a dict: {type(self.payload)}"
            )

        return {
            name: self.codec.from_primitive(self.payload[name], param_type) for name, param_type in spec.params.items()
        }
