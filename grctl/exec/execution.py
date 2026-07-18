import asyncio
import contextlib
import traceback
from datetime import UTC, datetime
from logging import Logger
from typing import Any, Protocol

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.context import Context
from grctl.exec.journal import Journal, StepHistoryAppender
from grctl.exec.kv_manager import KVManager
from grctl.exec.step_directive_factory import StepDirectiveFactory
from grctl.models import Directive, DirectiveKind, ErrorDetails, Fail, HistoryEvent, RunInfo, Step
from grctl.models.directive import NextMessage
from grctl.models.handler import HandlerConfig
from grctl.models.worker import WorkerInfo
from grctl.nats.connection import Connection


class StepDirectiveSender(Protocol):
    async def send(self, directive: Directive) -> None: ...


class Codec(Protocol):
    def from_primitive(self, value: Any, type: Any) -> Any: ...
    def to_primitive(self, value: Any) -> Any: ...


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

    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        worker_info: WorkerInfo,
        directive: Directive,
        handler_config: HandlerConfig,
        kvman: KVManager,
        history_appender: StepHistoryAppender,
        step_directive_sender: StepDirectiveSender,
        codec: Codec,
        logger: Logger,
        connection: Connection,
        step_history: list[HistoryEvent] | None = None,
    ) -> None:
        self.run_info = run_info
        self.worker_info = worker_info
        self.directive = directive
        self.handler_config = handler_config
        self.childs = ChildTracker()
        self.kvman = kvman
        self.step_history = step_history or []
        self.history_appender = history_appender
        self.directive_sender = step_directive_sender
        self.codec = codec
        self.logger = logger
        self.connection = connection
        self.step_directive_factory = StepDirectiveFactory(run_info, worker_info.id, directive)
        self.journal = Journal(self.step_history, self.history_appender)
        self.context = Context(
            self.journal, run_info, worker_info.id, connection, self.childs, self.parent_run
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
        try:
            self.is_executing = True
            if payload is None:
                outcome_directive = await handler(self.context)
            else:
                outcome_directive = await handler(self.context, **payload)
        except Exception as e:
            stack_trace = traceback.format_exc()
            self.logger.exception(f"Workflow execution failed for {self.step_name}")
            outcome_directive = self.step_directive_factory.step_result(
                DirectiveKind.fail,
                Fail(ErrorDetails(type=type(e).__name__, message=str(e), stack_trace=stack_trace)),
                datetime.now(UTC),
            )
        finally:
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
            await self.directive_sender.send(drc)

    async def send_step_result(self, directive: Directive) -> None:
        if not isinstance(directive.msg, NextMessage):
            raise ValueError("Wrong message kind for step result")  # noqa: TRY004

        if self.started_at is not None:
            duration_ms = int((datetime.now(UTC) - self.started_at).total_seconds() * 1000)

        pending_updates = self.kvman.get_pending_updates()
        if pending_updates:
            directive.kv_revs = pending_updates

        drc = self.step_directive_factory.step_result(
            next_msg_kind=directive.kind,
            next_msg=directive.msg,
            timestamp=datetime.now(UTC),
            duration_ms=duration_ms,
        )
        await self.directive_sender.send(drc)

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
