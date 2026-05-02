from __future__ import annotations

import dataclasses
import inspect
import logging
import typing
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar

from grctl.models.directive import Directive

logger = logging.getLogger(__name__)

_HandlerF = TypeVar("_HandlerF", bound=Callable[..., Awaitable[Directive]])


@dataclasses.dataclass
class HandlerSpec:
    params: dict[str, type]  # param name → resolved type, excludes ctx


def inspect_handler(fn: Callable[..., Any]) -> HandlerSpec:
    sig = inspect.signature(fn)
    # get_type_hints resolves string annotations produced by `from __future__ import annotations`
    hints = typing.get_type_hints(fn)

    params: dict[str, type] = {}
    first = True
    for name, param in sig.parameters.items():
        if first:
            first = False
            continue  # skip ctx
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise TypeError(f"Handler '{fn.__name__}' must not use *args or **kwargs")  # ty:ignore[unresolved-attribute]
        if name not in hints:
            raise TypeError(f"Handler '{fn.__name__}' parameter '{name}' must have a type annotation")  # ty:ignore[unresolved-attribute]
        params[name] = hints[name]

    return HandlerSpec(params=params)


@dataclasses.dataclass
class HandlerConfig:
    handler: Callable[..., Awaitable[Directive]]
    spec: HandlerSpec
    timeout: timedelta | None = None


class Workflow:
    """Workflow definition with decorator-based handler registration.

    Similar to FastAPI's app instance, this class allows you to define
    workflow handlers using decorators. The workflow instance is then
    registered with a Worker for execution.

    All handler functions receive a RunContext as their first parameter.

    Example:
        # Create workflow instance
        my_workflow = Workflow(name="my_workflow")

        # Define start handler
        @my_workflow.start()
        async def start(ctx: RunContext, input: dict) -> str:
            return "completed"

        # Define step handler
        @my_workflow.step()
        async def my_step(ctx: RunContext, message: str) -> None:
            logger.info(f"Notified: {message}")

    """

    def __init__(self, workflow_type: str) -> None:
        self._type = workflow_type
        self._start_handler: HandlerConfig | None = None
        self._run_handler: Callable[..., Any] | None = None
        self._step_handlers: dict[str, HandlerConfig] = {}
        self._on_event_handlers: dict[str, HandlerConfig] = {}
        self._update_handlers: dict[str, Callable[..., Any]] = {}
        self._query_handlers: dict[str, Callable[..., Any]] = {}

    @property
    def workflow_type(self) -> str:
        """Get the workflow type.

        Returns:
            The workflow type.

        Raises:
            ValueError: If workflow type has not been set.

        """
        if self._type is None:
            msg = "Workflow type not set. Provide type in constructor or decorate a function first."
            raise ValueError(msg)
        return self._type

    def start(self) -> Callable[[_HandlerF], _HandlerF]:
        """Decorate the workflow start handler.

        The start handler is called once when a workflow is first created.
        It's designed to initialize the workflow state. There can only be one start handler
        per workflow.

        Returns:
            Decorator function that registers the start handler.

        Raises:
            ValueError: If a start handler is already registered.

        Example:
            @workflow.start()
            async def start(ctx: RunContext, order_id: str) -> None:
                ctx.state.order_id = order_id
                ctx.state.items = []

        """

        def decorator(func: _HandlerF) -> _HandlerF:
            if self._start_handler is not None:
                msg = f"Workflow already has a start handler: {self._start_handler.handler.__name__}"  # ty:ignore[unresolved-attribute]
                raise ValueError(msg)

            spec = inspect_handler(func)
            self._start_handler = HandlerConfig(handler=func, spec=spec)
            logger.debug(f"Registered start handler for workflow: {self._type}")
            return func

        return decorator

    def step(
        self,
        timeout: timedelta | None = None,
    ) -> Callable[[_HandlerF], _HandlerF]:
        """Decorate the workflow step handler.

        Step represents a discrete unit of work within the workflow. Each step can create a checkpoint.
        There can be multiple step handlers per workflow, each identified by its function name.
        Each step handler must have a unique name.

        Args:
            timeout: Per-step timeout. Overrides the workflow-level timeout for this step.

        Returns:
            Decorator function that registers step handler.

        Raises:
            ValueError: If a step handler is already registered.

        Example:
            @workflow.step()
            async def my_step_name(ctx: RunContext, name: str) -> str:
                return f"Hello {name}"

        """

        def decorator(func: _HandlerF) -> _HandlerF:
            if self._step_handlers.get(func.__name__) is not None:
                msg = f"Step handler '{func.__name__}' already registered"
                raise ValueError(msg)

            step_timeout = timeout if timeout is not None else timedelta(seconds=10)
            spec = inspect_handler(func)

            self._step_handlers[func.__name__] = HandlerConfig(
                handler=func,
                spec=spec,
                timeout=step_timeout,
            )
            logger.debug(f"Registered step handler '{func.__name__}' for workflow: {self._type}")
            return func

        return decorator

    def event(
        self,
        name: str | None = None,
    ) -> Callable[[_HandlerF], _HandlerF]:
        """Decorate workflow event handlers.

        Events are asynchronous, fire-and-forget notifications sent to
        a running workflow. They can mutate workflow state.

        Args:
            name: Optional event name. If not provided, uses function name.

        Returns:
            Decorator function that registers the on_event handler.

        Raises:
            ValueError: If an event with this name is already registered.

        Example:
            @workflow.event()
            async def approve(ctx: RunContext) -> None:
                # Mutate state
                pass

            @workflow.event(name="custom_event")
            async def my_handler(ctx: RunContext, data: str) -> None:
                logger.info(f"Event data: {data}")

        """

        def decorator(func: _HandlerF) -> _HandlerF:
            event_name = name or func.__name__

            if event_name in self._on_event_handlers:
                msg = f"Event '{event_name}' already registered"
                raise ValueError(msg)

            spec = inspect_handler(func)
            self._on_event_handlers[event_name] = HandlerConfig(handler=func, spec=spec)
            logger.debug(
                f"Registered on_event handler '{event_name}' for workflow: {self._type or 'unnamed'}",
            )
            return func

        return decorator

    def query(
        self,
        name: str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorate workflow query handlers.

        Queries are read-only operations that return workflow state
        without modifying it.

        Args:
            name: Optional query name. If not provided, uses function name.

        Returns:
            Decorator function that registers the query handler.

        Raises:
            ValueError: If a query with this name is already registered.

        Example:
            @workflow.query()
            async def get_status(ctx: RunContext) -> str:
                return "running"

        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            query_name = name or func.__name__  # ty:ignore[unresolved-attribute]

            if query_name in self._query_handlers:
                msg = f"Query '{query_name}' already registered"
                raise ValueError(msg)

            self._query_handlers[query_name] = func

            logger.debug(
                f"Registered query handler '{query_name}' for workflow: {self._type or 'unnamed'}",
            )
            return func

        return decorator
