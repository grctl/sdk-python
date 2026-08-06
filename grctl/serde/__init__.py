"""Serialisation of user-defined types to and from durable workflow history.

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
    MalformedEnvelopeError,
    NativeTypeError,
    SerdeError,
    SerializerConflictError,
    TagMismatchError,
    UnsupportedTypeError,
    VersionMismatchError,
)
from grctl.serde.fingerprint import fingerprint
from grctl.serde.registry import SerializerRegistry
from grctl.serde.serializer import MigratingSerializer, Serializer, TypeSerializer

__all__ = [
    "MalformedEnvelopeError",
    "MigratingSerializer",
    "NativeTypeError",
    "SerdeError",
    "Serializer",
    "SerializerConflictError",
    "SerializerRegistry",
    "TagMismatchError",
    "TypeSerializer",
    "UnsupportedTypeError",
    "VersionMismatchError",
    "decode",
    "default_registry",
    "encode",
    "fingerprint",
    "register",
    "register_predicate",
    "serializer",
]
