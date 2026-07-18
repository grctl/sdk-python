from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol, TypeVar

from grctl.models import Directive


class Handler(Protocol):
    __name__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[Directive]: ...


HandlerF = TypeVar("HandlerF", bound=Handler)


@dataclass
class HandlerSpec:
    params: dict[str, type]  # param name → resolved type, excludes ctx


@dataclass
class HandlerConfig:
    handler: Callable[..., Awaitable[Directive]]
    spec: HandlerSpec
    timeout: timedelta | None = None
    on_timeout_handler: Callable[..., Awaitable[Directive]] | None = None
