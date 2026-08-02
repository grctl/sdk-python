from __future__ import annotations

import inspect
import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from grctl.models.command import StepDef, WorkflowTypeDef
from grctl.models.handler import HandlerConfig, HandlerF, HandlerSpec, StepKind


def get_handler_spec(fn: Callable[..., Any]) -> HandlerSpec:
    sig = inspect.signature(fn)
    hints = typing.get_type_hints(fn)

    params: dict[str, type] = {}
    first = True
    for name, param in sig.parameters.items():
        if first:
            first = False
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise TypeError(f"Handler '{fn.__name__}' must not use *args or **kwargs")  # ty:ignore[unresolved-attribute]
        if name not in hints:
            raise TypeError(f"Handler '{fn.__name__}' parameter '{name}' must have a type annotation")  # ty:ignore[unresolved-attribute]
        params[name] = hints[name]

    return HandlerSpec(params=params)


@dataclass(frozen=True)
class StepInfo:
    """Runtime metadata needed to construct a transition to a registered step."""

    timeout_ms: int


class Workflow:
    """Workflow definition with one registry for all mutating step handlers.

    A step is internal by default. ``start=True`` marks the workflow creation
    step, and ``event=True`` makes the step externally triggerable.
    """

    def __init__(self, workflow_type: str) -> None:
        self._type = workflow_type
        self._steps: dict[str, HandlerConfig] = {}
        self._query_handlers: dict[str, Callable[..., Any]] = {}

    @property
    def workflow_type(self) -> str:
        """Return the registered workflow type."""
        if self._type is None:
            msg = "Workflow type not set. Provide type in constructor or decorate a function first."
            raise ValueError(msg)
        return self._type

    @property
    def start_step_name(self) -> str | None:
        """Return the registered start-step name, if one exists."""
        for name, config in self._steps.items():
            if config.kind is StepKind.start:
                return name
        return None

    @property
    def step_names(self) -> list[str]:
        """Return the names of all registered steps."""
        return list(self._steps)

    @property
    def event_names(self) -> list[str]:
        """Return the names of externally triggerable steps."""
        return [name for name, config in self._steps.items() if config.kind is StepKind.external]

    @property
    def query_names(self) -> list[str]:
        """Return the names of registered query handlers."""
        return list(self._query_handlers)

    def step_handler(self, name: str) -> HandlerConfig:
        """Look up a registered step handler by name."""
        config = self._steps.get(name)
        if config is None:
            raise ValueError(f"Step handler '{name}' not registered in workflow '{self.workflow_type}'")
        return config

    @property
    def step_infos(self) -> Mapping[str, StepInfo]:
        """Return registered step metadata used by directive construction."""
        return {
            name: StepInfo(timeout_ms=int(config.timeout.total_seconds() * 1000) if config.timeout else 0)
            for name, config in self._steps.items()
        }

    def type_def(self) -> WorkflowTypeDef:
        """Build the structural definition reported to the server."""
        step_defs = [
            StepDef(
                name=name,
                timeout_ms=int(config.timeout.total_seconds() * 1000) if config.timeout else 0,
                external=config.kind is StepKind.external,
            )
            for name, config in self._steps.items()
        ]
        return WorkflowTypeDef(
            type=self.workflow_type,
            start_step=self.start_step_name or "",
            step_defs=step_defs,
            queries=self.query_names,
        )

    def step(
        self,
        *,
        start: bool = False,
        event: bool = False,
        name: str | None = None,
        timeout: timedelta | None = None,
    ) -> Callable[[HandlerF], HandlerF]:
        """Decorate a workflow step handler.

        Steps are internal continuations by default. Set ``start=True`` for
        the workflow creation step or ``event=True`` for an externally
        triggerable step. ``timeout`` is the handler execution timeout; when
        omitted, the server applies its configured default.
        """

        def decorator(func: HandlerF) -> HandlerF:
            if start and event:
                raise ValueError("A step cannot be both a start step and an event step")

            step_name = name or func.__name__
            if step_name in self._steps:
                raise ValueError(f"Step handler '{step_name}' already registered")
            if start and self.start_step_name is not None:
                raise ValueError(f"Workflow already has a start handler: '{self.start_step_name}'")

            kind = StepKind.start if start else StepKind.external if event else StepKind.internal
            self._steps[step_name] = HandlerConfig(
                handler=func,
                spec=get_handler_spec(func),
                kind=kind,
                timeout=timeout,
            )
            func.__grctl_step_name__ = step_name  # ty:ignore[unresolved-attribute]
            return func

        return decorator

    def query(
        self,
        name: str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorate a read-only workflow query handler."""

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            query_name = name or func.__name__  # ty:ignore[unresolved-attribute]
            if query_name in self._query_handlers:
                raise ValueError(f"Query '{query_name}' already registered")
            self._query_handlers[query_name] = func
            return func

        return decorator
