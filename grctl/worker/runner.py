import functools
import traceback
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from grctl.worker.context import Context

from grctl.logging_config import get_logger
from grctl.models import (
    Directive,
    DirectiveKind,
    ErrorDetails,
    Event,
    Start,
    Step,
    StepPickedUp,
)
from grctl.models.directive import StepResult
from grctl.models.run_info_helper import RunInfoManager
from grctl.worker.errors import NextDirectiveMissingError
from grctl.worker.runtime import StepRuntime, set_step_runtime
from grctl.workflow.workflow import HandlerConfig

logger = get_logger(__name__)


def workflow_error_handler(func):  # noqa: ANN001, ANN201
    """Handle exceptions in workflow methods and publish fail directive to server."""

    @functools.wraps(func)
    async def wrapper(self, *args: Any, **kwargs: Any):  # noqa: ANN001, ANN202
        try:
            return await func(self, *args, **kwargs)
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.exception(f"Workflow execution failed in {func.__name__}")

            ctx = self.runtime.get_step_context()
            fail_directive = ctx.next.fail_step(
                step_name=self.runtime.step_name,
                error=ErrorDetails(
                    type=type(e).__name__,
                    message=str(e),
                    stack_trace=stack_trace,
                ),
            )
            await self.runtime.publisher.publish_next_directive(
                self.runtime.run_info, fail_directive, enc_hook=self.runtime.codec.enc_hook
            )
            raise

    return wrapper


class WorkflowRunner:
    """Orchestrates workflow run lifecycle."""

    _result = None

    def __init__(self, runtime: StepRuntime) -> None:
        self.runtime = runtime
        self._runtime_token = set_step_runtime(runtime)
        self.workflow = runtime.workflow

    async def handle_directive(self, directive: Directive) -> None:
        """Dispatch directive to appropriate handler."""
        msg = directive.msg
        if isinstance(msg, Start):
            await self.handle_start(msg.input)
        elif isinstance(msg, Event):
            await self.handle_event(msg.event_name, msg.payload)
        elif isinstance(msg, Step):
            await self.handle_step(msg)
        else:
            logger.warning(f"Unknown command type: {type(directive)}")

    @workflow_error_handler
    async def handle_start(self, payload: Any | None) -> None:
        handler_config = self.workflow.start_handler
        if handler_config is None:
            raise ValueError("Workflow start handler is not defined.")

        self.runtime.run_info = RunInfoManager.start(self.runtime.run_info, datetime.now(UTC))
        self.runtime.step_name = "start"
        await self._execute_step(handler_config, payload)

    @workflow_error_handler
    async def handle_event(self, event_name: str, payload: Any | None) -> None:
        handler_config = self._resolve_event_handler(event_name)
        if handler_config is None:
            logger.warning(f"No handler registered for event '{event_name}'")
            return

        self.runtime.step_name = event_name
        await self._execute_step(handler_config, payload)

    def _resolve_event_handler(self, event_name: str) -> HandlerConfig | None:
        # Inbound events resolve to an event handler by name. A child-completion callback
        # is delivered the same way but names a step handler (on_completed_step), so fall
        # back to step handlers. Event handlers take precedence to preserve existing behavior.
        handler_config = self.workflow._on_event_handlers.get(event_name)  # noqa: SLF001
        if handler_config is not None:
            return handler_config
        return self.workflow._step_handlers.get(event_name)  # noqa: SLF001

    @workflow_error_handler
    async def handle_step(self, step: Step) -> None:
        logger.debug(f"Executing step: {step.step_name} for run {self.runtime.run_info.id}")

        step_config = self.workflow._step_handlers.get(step.step_name)  # noqa: SLF001
        if step_config is None:
            raise ValueError(f"Step handler '{step.step_name}' is not defined.")
        self.runtime.step_name = step.step_name
        await self._execute_step(step_config, None)

    def _get_event_name(self, handler: Any) -> str | None:
        for event_name, event_config in self.workflow._on_event_handlers.items():  # noqa: SLF001
            if handler == event_config.handler:
                return event_name
        return None

    async def _execute_step(self, handler_config: HandlerConfig, payload: Any | None) -> None:
        ctx = self.runtime.get_step_context()
        start_time = datetime.now(UTC)

        await self._publish_step_picked_up()

        try:
            directive = await self._invoke_handler(ctx, handler_config, payload)
            await self._publish_next_directive(directive, start_time)
        finally:
            # Always release child handles started in this step, even when the handler
            # raised, so an unawaited future never warns or leaks its subscription.
            await ctx.discard_started_handles()

    async def _invoke_handler(self, ctx: "Context", handler_config: HandlerConfig, payload: Any | None) -> Directive:
        spec = handler_config.spec
        handler = handler_config.handler
        if not spec.params or payload is None:
            return await handler(ctx)

        # Single param: if payload is already keyed by param name use the value,
        # otherwise treat payload itself as the value (e.g. bare Pydantic model).
        if len(spec.params) == 1:
            name, param_type = next(iter(spec.params.items()))
            raw = payload[name] if isinstance(payload, dict) and name in payload else payload
            typed_value = self.runtime.codec.from_primitive(raw, param_type)
            return await handler(ctx, **{name: typed_value})

        # Multi param: convert each param from the payload dict and pass as kwargs
        if not isinstance(payload, dict):
            raise TypeError(f"Handler expects params {list(spec.params)} but payload is not a dict: {type(payload)}")
        typed_kwargs = {
            name: self.runtime.codec.from_primitive(payload[name], param_type)
            for name, param_type in spec.params.items()
        }
        return await handler(ctx, **typed_kwargs)

    async def _publish_step_picked_up(self) -> None:
        if self.runtime.step_history is None or len(self.runtime.step_history) == 0:
            now = datetime.now(UTC)
            directive = Directive(
                id=str(uuid.uuid4()),
                timestamp=now,
                kind=DirectiveKind.step_picked_up,
                run_info=self.runtime.run_info,
                msg=StepPickedUp(
                    step_name=self.runtime.step_name,
                    worker_id=self.runtime.worker_id,
                    timestamp=now,
                ),
            )
            await self.runtime.publisher.publish_next_directive(
                self.runtime.run_info, directive, enc_hook=self.runtime.codec.enc_hook
            )

    async def _publish_next_directive(
        self,
        directive: Directive,
        step_start_time: datetime | None = None,
    ) -> None:
        if not isinstance(directive, Directive):
            raise NextDirectiveMissingError(f"Step did not return a Directive. {directive=}", self.runtime.step_name)

        if step_start_time is not None and isinstance(directive.msg, StepResult):
            directive.msg.duration_ms = int((datetime.now(UTC) - step_start_time).total_seconds() * 1000)

        pending_updates = self.runtime.store.get_pending_updates()
        if pending_updates:
            directive.kv_revs = pending_updates

        await self.runtime.publisher.publish_next_directive(
            self.runtime.run_info, directive, enc_hook=self.runtime.codec.enc_hook
        )
