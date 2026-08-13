"""The registry that maps user types to primitive converters."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from grctl.serde import builtins
from grctl.serde.errors import NativeTypeError, SerializerConflictError, UnsupportedTypeError
from grctl.serde.serializer import Serializer, TypeMatcher, is_serializer

type EncodeFn = Callable[[Any], Any]
type DecodeFn = Callable[[type, Any], Any]


@dataclass(frozen=True, slots=True)
class Registration:
    """A converter bound to one exact type."""

    type: type
    serializer: Serializer[Any]


@dataclass(frozen=True, slots=True)
class PredicateRegistration:
    """A serialiser for an open family of types, matched by predicate.

    The escape hatch behind the built-in pydantic support: a rule that claims
    every subclass of some base rather than one named type.
    """

    matches: TypeMatcher
    encode: EncodeFn
    decode: DecodeFn


def _matches(predicate: PredicateRegistration, tp: type) -> bool:
    """Test a predicate without letting non-class targets blow up the lookup.

    `issubclass` raises on generic aliases and other typing constructs, which
    reach here as ordinary annotation values.
    """
    if not isinstance(tp, type):
        return False
    try:
        return predicate.matches(tp)
    except TypeError:
        return False


class SerializerRegistry:
    """Resolves user types to primitive converters.

    Lookup is exact type first, then predicate rules in registration order.
    Exact registrations override a built-in rule; among predicates, the first
    registration wins.
    """

    def __init__(self, *, include_builtins: bool = True) -> None:
        self._by_type: dict[type, Registration] = {}
        self._predicates: list[PredicateRegistration] = []

        if include_builtins:
            self.register_predicate(
                matches=builtins.matches_pydantic_model,
                encode=builtins.encode_pydantic_model,
                decode=builtins.decode_pydantic_model,
            )

    def register(
        self,
        tp: type,
        serializer: Serializer[Any],
        *,
        override: bool = False,
    ) -> None:
        """Bind a serialiser to one exact type.

        Conflicts and invalid serializers raise at registration time rather than
        on the workflow path.
        """
        if not builtins.is_hookable(tp):
            raise NativeTypeError(tp)
        if not is_serializer(serializer):
            raise TypeError(f"{serializer!r} must define both `encode` and `decode` to serialise {tp!r}")

        existing = self._by_type.get(tp)
        if existing is not None and not override:
            raise SerializerConflictError(
                f"{tp!r} already has serialiser {type(existing.serializer)!r}. Pass override=True to replace it."
            )

        self._by_type[tp] = Registration(type=tp, serializer=serializer)

    def register_predicate(
        self,
        *,
        matches: TypeMatcher,
        encode: EncodeFn,
        decode: DecodeFn,
    ) -> None:
        """Register a rule claiming an open family of types."""
        self._predicates.append(PredicateRegistration(matches=matches, encode=encode, decode=decode))

    def supports(self, tp: type) -> bool:
        return tp in self._by_type or any(_matches(p, tp) for p in self._predicates)

    def encode(self, obj: Any) -> Any:
        """Convert a user value to msgpack-native primitives."""
        tp = type(obj)

        registration = self._by_type.get(tp)
        if registration is not None:
            return registration.serializer.encode(obj)

        for predicate in self._predicates:
            if _matches(predicate, tp):
                return predicate.encode(obj)

        raise UnsupportedTypeError(tp)

    def decode(self, tp: type, raw: Any) -> Any:
        """Cast primitives into the requested user type."""
        registration = self._by_type.get(tp)
        if registration is not None:
            return registration.serializer.decode(raw)

        for predicate in self._predicates:
            if _matches(predicate, tp):
                return predicate.decode(tp, raw)

        raise UnsupportedTypeError(tp)
