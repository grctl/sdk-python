"""Serialisation rules every registry starts with.

Kept out of `SerializerRegistry` itself so the registry stays a pure lookup
structure with no knowledge of any particular library.
"""

from typing import Any

import msgspec.inspect
from pydantic import BaseModel


def is_hookable(tp: type) -> bool:
    """Whether the wire codec would consult a serialiser for `tp` at all.

    msgspec encodes the types it understands directly and only calls
    enc_hook/dec_hook for what it classifies as a custom type.
    """
    try:
        return isinstance(msgspec.inspect.type_info(tp), msgspec.inspect.CustomType)
    except (TypeError, NotImplementedError):
        # Inspection failed rather than proved the type native; let it register.
        return True


def matches_pydantic_model(tp: type) -> bool:
    return issubclass(tp, BaseModel)


def encode_pydantic_model(obj: Any) -> Any:
    return obj.model_dump()


def decode_pydantic_model(tp: type, raw: Any) -> Any:
    return tp.model_validate(raw)  # ty:ignore[unresolved-attribute]
