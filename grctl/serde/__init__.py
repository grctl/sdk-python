"""Conversion of user-defined types to and from msgpack-native primitives.

Users register serialisers for their own types with `@serializer(MyType)`. The
transport layer converts the resulting primitives to bytes; it knows nothing
about the types themselves.
"""

from grctl.serde.api import (
    decode,
    default_registry,
    encode,
    register,
    register_predicate,
    serializer,
)
from grctl.serde.errors import (
    NativeTypeError,
    SerdeError,
    SerializerConflictError,
    UnsupportedTypeError,
)
from grctl.serde.fingerprint import fingerprint
from grctl.serde.registry import SerializerRegistry
from grctl.serde.serializer import Serializer, TypeSerializer

__all__ = [
    "NativeTypeError",
    "SerdeError",
    "Serializer",
    "SerializerConflictError",
    "SerializerRegistry",
    "TypeSerializer",
    "UnsupportedTypeError",
    "decode",
    "default_registry",
    "encode",
    "fingerprint",
    "register",
    "register_predicate",
    "serializer",
]
