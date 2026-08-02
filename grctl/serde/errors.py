"""Errors raised while serialising user types to and from durable history."""


class SerdeError(Exception):
    """Base class for every serialisation failure."""


class UnsupportedTypeError(SerdeError):
    """No serialiser is registered for a value's type."""

    def __init__(self, tp: type) -> None:
        super().__init__(f"No serialiser registered for type {tp!r}. Register one with @grctl.serializer({tp!r}).")
        self.type = tp


class SerializerConflictError(SerdeError):
    """Two serialisers claim the same type or the same wire tag."""


class NativeTypeError(SerdeError):
    """A serialiser was registered for a type the wire codec already encodes itself.

    msgspec encodes dataclasses, Structs, enums, NamedTuples, Decimal, UUID,
    datetime and the builtin scalars and containers directly, and never consults
    a serialiser for them. Registering one would be a silent no-op, so it is
    rejected at registration instead.
    """

    def __init__(self, tp: type) -> None:
        super().__init__(
            f"{tp!r} is encoded natively by the wire codec, so a serialiser for it would never be called. "
            "Remove the registration and rely on the native encoding, or wrap the value in a type "
            "the codec does not know."
        )
        self.type = tp


class MalformedEnvelopeError(SerdeError):
    """A durable value is missing the envelope a registered serialiser writes."""

    def __init__(self, tp: type, raw: object) -> None:
        super().__init__(
            f"Cannot decode {tp!r}: expected a tagged value written by its serialiser, got {type(raw)!r}. "
            "The value was likely written before a serialiser was registered for this type."
        )
        self.type = tp
        self.raw = raw


class TagMismatchError(SerdeError):
    """A durable value carries a different type tag than the target type's serialiser."""

    def __init__(self, expected: str, found: str, tp: type) -> None:
        super().__init__(f"Cannot decode {tp!r}: value was written as {found!r}, expected {expected!r}.")
        self.expected = expected
        self.found = found
        self.type = tp


class VersionMismatchError(SerdeError):
    """A durable value was written by a different version of its serialiser.

    Raised when a serialiser does not implement `migrate` for the older format.
    """

    def __init__(self, tag: str, found: int, current: int) -> None:
        super().__init__(
            f"Value tagged {tag!r} was written at version {found}, current serialiser is at version {current}. "
            "Implement `migrate(raw, from_version)` on the serialiser to upgrade older values."
        )
        self.tag = tag
        self.found = found
        self.current = current
