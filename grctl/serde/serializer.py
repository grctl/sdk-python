"""The serialiser contract users implement for their own types."""

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

type TypeMatcher = Callable[[type], bool]


@runtime_checkable
class Serializer[T](Protocol):
    """Converts one user type to and from msgpack-native primitives."""

    def encode(self, value: T) -> Any: ...

    def decode(self, raw: Any) -> T: ...


class TypeSerializer[T]:
    """Base class for serialisers, for users who prefer inheritance to the protocol.

    Subclassing is optional — `@serializer` accepts any object with `encode`
    and `decode`.
    """

    def encode(self, value: T) -> Any:
        raise NotImplementedError

    def decode(self, raw: Any) -> T:
        raise NotImplementedError


def is_serializer(obj: object) -> bool:
    return callable(getattr(obj, "encode", None)) and callable(getattr(obj, "decode", None))
