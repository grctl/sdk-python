"""The registry that maps user types to their serialisers."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from grctl.logging_config import get_logger
from grctl.serde import builtins, envelope
from grctl.serde.errors import (
    MalformedEnvelopeError,
    NativeTypeError,
    SerializerConflictError,
    TagMismatchError,
    UnsupportedTypeError,
    VersionMismatchError,
)
from grctl.serde.serializer import Serializer, TagFor, TypeMatcher, default_tag

logger = get_logger(__name__)

type EncodeFn = Callable[[Any], Any]
type DecodeFn = Callable[[type, Any], Any]


@dataclass(frozen=True, slots=True)
class Registration:
    """A serialiser bound to one exact type."""

    type: type
    tag: str
    version: int
    serializer: Serializer[Any]


@dataclass(frozen=True, slots=True)
class PredicateRegistration:
    """A serialiser for an open family of types, matched by predicate.

    The escape hatch behind the built-in pydantic support: a rule that claims
    every subclass of some base rather than one named type.
    """

    matches: TypeMatcher
    tag_for: TagFor
    version: int
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
    """Resolves user types to serialisers, and tags to types.

    Lookup is exact type first, then predicate rules in registration order.
    Exact registrations therefore always win, which is how a user overrides a
    built-in rule; among predicates the order is first-registered-wins so that
    the resolution never depends on module import order.
    """

    def __init__(self, *, include_builtins: bool = True) -> None:
        self._by_type: dict[type, Registration] = {}
        self._by_tag: dict[str, Registration] = {}
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
        tag: str | None = None,
        version: int = 1,
        override: bool = False,
    ) -> None:
        """Bind a serialiser to one exact type.

        Conflicts raise at registration time rather than mid-run at encode time.
        """
        if not builtins.is_hookable(tp):
            raise NativeTypeError(tp)

        resolved_tag = tag or default_tag(tp)

        existing = self._by_type.get(tp)
        if existing is not None and not override:
            raise SerializerConflictError(
                f"{tp!r} already has serialiser {type(existing.serializer)!r}. Pass override=True to replace it."
            )

        tag_owner = self._by_tag.get(resolved_tag)
        if tag_owner is not None and tag_owner.type is not tp:
            raise SerializerConflictError(
                f"Tag {resolved_tag!r} is already used by {tag_owner.type!r}; "
                f"give {tp!r} an explicit tag to keep durable values distinguishable."
            )

        registration = Registration(type=tp, tag=resolved_tag, version=version, serializer=serializer)
        if existing is not None:
            self._by_tag.pop(existing.tag, None)
        self._by_type[tp] = registration
        self._by_tag[resolved_tag] = registration

    def register_predicate(
        self,
        *,
        matches: TypeMatcher,
        encode: EncodeFn,
        decode: DecodeFn,
        tag_for: TagFor = default_tag,
        version: int = 1,
    ) -> None:
        """Register a rule claiming an open family of types."""
        self._predicates.append(
            PredicateRegistration(matches=matches, tag_for=tag_for, version=version, encode=encode, decode=decode)
        )

    def supports(self, tp: type) -> bool:
        return tp in self._by_type or any(_matches(p, tp) for p in self._predicates)

    def encode(self, obj: Any) -> dict[str, Any]:
        """Encode a value into its tagged envelope."""
        tp = type(obj)

        registration = self._by_type.get(tp)
        if registration is not None:
            return envelope.wrap(registration.tag, registration.version, registration.serializer.encode(obj))

        for predicate in self._predicates:
            if _matches(predicate, tp):
                return envelope.wrap(predicate.tag_for(tp), predicate.version, predicate.encode(obj))

        raise UnsupportedTypeError(tp)

    def decode(self, tp: type, raw: Any) -> Any:
        """Decode a tagged envelope back into `tp`."""
        registration = self._by_type.get(tp)
        if registration is not None:
            payload = self._open(raw, tp, registration.tag, registration.version, registration.serializer)
            return registration.serializer.decode(payload)

        for predicate in self._predicates:
            if _matches(predicate, tp):
                payload = self._open(raw, tp, predicate.tag_for(tp), predicate.version, None)
                return predicate.decode(tp, payload)

        raise UnsupportedTypeError(tp)

    def rehydrate(self, raw: Any) -> Any:
        """Best-effort decode of a value whose target type is unknown.

        Used where a value is read back without an annotation to drive it — an
        untyped KV read, a task result handed back as `Any`. Envelopes with a
        known tag become objects again; unknown tags degrade to their payload
        rather than leaking wrapper dicts into user code.
        """
        if isinstance(raw, list):
            return [self.rehydrate(item) for item in raw]

        if not isinstance(raw, dict):
            return raw

        if not envelope.is_envelope(raw):
            return {key: self.rehydrate(value) for key, value in raw.items()}

        tag = envelope.read_tag(raw)
        registration = self._by_tag.get(tag)
        if registration is None:
            logger.debug(f"No serialiser registered for tag {tag!r}; returning its payload untyped")
            return self.rehydrate(envelope.read_payload(raw))

        return self.decode(registration.type, raw)

    def _open(
        self,
        raw: Any,
        tp: type,
        expected_tag: str,
        current_version: int,
        serializer: Serializer[Any] | None,
    ) -> Any:
        """Validate an envelope's tag and version, and return its payload."""
        if not envelope.is_envelope(raw):
            raise MalformedEnvelopeError(tp, raw)

        found_tag = envelope.read_tag(raw)
        if found_tag != expected_tag:
            raise TagMismatchError(expected_tag, found_tag, tp)

        payload = self.rehydrate(envelope.read_payload(raw))

        found_version = envelope.read_version(raw)
        if found_version == current_version:
            return payload

        migrate = getattr(serializer, "migrate", None)
        if migrate is None:
            raise VersionMismatchError(expected_tag, found_version, current_version)
        return migrate(payload, found_version)
