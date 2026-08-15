from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum, auto
from typing import Any, Protocol, TypeVar

from grctl.models import Directive


class Handler(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[Directive]: ...


HandlerF = TypeVar("HandlerF", bound=Handler)


class StepKind(StrEnum):
    """A workflow step's role in server registration."""

    start = auto()
    event = auto()
    step = auto()


@dataclass
class HandlerSpec:
    payload_parameters: dict[str, type]  # Parameter name → resolved type, excludes ctx.
    defaulted_payload_parameters: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class RegisteredStep:
    """A handler registered to serve one workflow step."""

    handler: Callable[..., Awaitable[Directive]]
    spec: HandlerSpec
    kind: StepKind = StepKind.step
    timeout: timedelta | None = None
