"""The serialiser contract users implement for their own types."""

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

type TypeMatcher = Callable[[type], bool]
type TagFor = Callable[[type], str]


@runtime_checkable
class Serializer[T](Protocol):
    """Converts one user type to and from primitives that survive durable history.

    `encode` must return only msgpack-native primitives (str, int, float, bool,
    None, list, dict) or values of other registered types. `decode` receives
    exactly what `encode` produced.

    The encoded shape is a durable contract: values written by this serialiser
    are replayed by later builds of the same workflow. To change the shape,
    raise `version` and implement `migrate` so older values still decode.
    """

    def encode(self, value: T) -> Any: ...

    def decode(self, raw: Any) -> T: ...


@runtime_checkable
class MigratingSerializer[T](Serializer[T], Protocol):
    """A serialiser that can upgrade values written by an earlier version."""

    def migrate(self, raw: Any, from_version: int) -> Any:
        """Return `raw` rewritten into the current version's encoded shape."""
        ...


class TypeSerializer[T]:
    """Base class for serialisers, for users who prefer inheritance to the protocol.

    Subclassing is optional — `@serializer` accepts any object with `encode`
    and `decode`.
    """

    def encode(self, value: T) -> Any:
        raise NotImplementedError

    def decode(self, raw: Any) -> T:
        raise NotImplementedError


def default_tag(tp: type) -> str:
    """Wire tag used when a serialiser does not declare one.

    The bare class name, not the qualified path: moving a class between modules
    is a refactor, and it must not invalidate durable history.
    """
    return tp.__name__


def is_serializer(obj: object) -> bool:
    return callable(getattr(obj, "encode", None)) and callable(getattr(obj, "decode", None))
