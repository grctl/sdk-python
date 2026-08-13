"""Errors raised while serialising user types to and from durable history."""


class SerdeError(Exception):
    """Base class for every serialisation failure."""


class UnsupportedTypeError(SerdeError):
    """No serialiser is registered for a value's type."""

    def __init__(self, tp: type) -> None:
        super().__init__(f"No serialiser registered for type {tp!r}. Register one with @grctl.serializer({tp!r}).")
        self.type = tp


class SerializerConflictError(SerdeError):
    """Two serialisers claim the same type."""


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
