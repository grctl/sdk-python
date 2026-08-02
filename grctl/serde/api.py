"""Public registration surface for user-defined serialisers."""

from collections.abc import Callable
from typing import Any

from grctl.serde.registry import DecodeFn, EncodeFn, SerializerRegistry
from grctl.serde.serializer import Serializer, TagFor, TypeMatcher, default_tag, is_serializer

_default_registry = SerializerRegistry()


def default_registry() -> SerializerRegistry:
    """Return the process-wide registry used when no explicit one is passed."""
    return _default_registry


def serializer[T](
    tp: type[T],
    *,
    tag: str | None = None,
    version: int = 1,
    registry: SerializerRegistry | None = None,
    override: bool = False,
) -> Callable[[type], type]:
    """Register the decorated class as the serialiser for `tp`.

    The class must be constructible with no arguments and provide `encode` and
    `decode`. Registration happens at class-definition time, so the module
    defining the serialiser must be imported before a worker or client starts.

        @serializer(Money)
        class MoneySerializer:
            def encode(self, value: Money) -> Any:
                return {"amount": str(value.amount), "currency": value.currency}

            def decode(self, raw: Any) -> Money:
                return Money(Decimal(raw["amount"]), raw["currency"])

    Args:
        tp: The type this serialiser handles. Matching is exact, not by subclass.
        tag: Wire tag written into durable history. Defaults to the type's name;
            set it explicitly to keep history readable across a class rename.
        version: Encoding version. Raise it when the encoded shape changes, and
            implement `migrate(raw, from_version)` to upgrade older values.
        registry: Registry to register into. Defaults to the process-wide one.
        override: Replace an existing serialiser for `tp` instead of raising.

    """

    def decorate(cls: type) -> type:
        if not is_serializer(cls):
            raise TypeError(f"{cls!r} must define both `encode` and `decode` to serialise {tp!r}")
        target = registry if registry is not None else _default_registry
        target.register(tp, cls(), tag=tag, version=version, override=override)
        return cls

    return decorate


def register[T](  # noqa: PLR0913
    tp: type[T],
    serializer_instance: Serializer[T],
    *,
    tag: str | None = None,
    version: int = 1,
    registry: SerializerRegistry | None = None,
    override: bool = False,
) -> None:
    """Register an already-constructed serialiser — the non-decorator form."""
    target = registry if registry is not None else _default_registry
    target.register(tp, serializer_instance, tag=tag, version=version, override=override)


def register_predicate(  # noqa: PLR0913
    *,
    matches: TypeMatcher,
    encode: EncodeFn,
    decode: DecodeFn,
    tag_for: TagFor = default_tag,
    version: int = 1,
    registry: SerializerRegistry | None = None,
) -> None:
    """Register a rule claiming an open family of types, e.g. every subclass of a base.

    The escape hatch for cases a per-type serialiser cannot express. Exact
    registrations always take precedence over predicate rules.
    """
    target = registry if registry is not None else _default_registry
    target.register_predicate(matches=matches, encode=encode, decode=decode, tag_for=tag_for, version=version)


def encode(value: Any, *, registry: SerializerRegistry | None = None) -> Any:
    """Encode a single value through a registry — mainly useful in tests."""
    target = registry if registry is not None else _default_registry
    return target.encode(value)


def decode[T](tp: type[T], raw: Any, *, registry: SerializerRegistry | None = None) -> T:
    """Decode a single tagged value through a registry — mainly useful in tests."""
    target = registry if registry is not None else _default_registry
    return target.decode(tp, raw)
