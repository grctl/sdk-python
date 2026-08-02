"""msgpack encoding for the wire.

Knows how to turn values into bytes and back. It does not know how any user
type is represented — that belongs to `grctl.serde`, which this module reaches
through the narrow `PrimitiveConverter` port.
"""

from typing import Any, Protocol

import msgspec
import msgspec.msgpack

from grctl.serde import default_registry


class PrimitiveConverter(Protocol):
    """Converts user types to and from msgpack-native primitives."""

    def encode(self, obj: Any) -> Any: ...

    def decode(self, tp: type, raw: Any) -> Any: ...

    def rehydrate(self, raw: Any) -> Any: ...


class MsgspecCodec:
    """Bridges msgspec's enc_hook/dec_hook onto a serialiser registry."""

    def __init__(self, serializers: PrimitiveConverter | None = None) -> None:
        self._serializers: PrimitiveConverter = serializers if serializers is not None else default_registry()

    @property
    def serializers(self) -> PrimitiveConverter:
        return self._serializers

    def enc_hook(self, obj: Any) -> Any:
        return self._serializers.encode(obj)

    def dec_hook(self, tp: type, obj: Any) -> Any:
        return self._serializers.decode(tp, obj)

    def to_primitive(self, value: Any) -> Any:
        return msgspec.to_builtins(value, enc_hook=self.enc_hook)

    def from_primitive(self, raw: Any, tp: type | None = None) -> Any:
        """Convert primitives back into `tp`.

        Without a target type the tags carried in the value drive the decode as
        far as they can, so callers never see raw envelopes.
        """
        if tp is None or tp is Any:
            return self._serializers.rehydrate(raw)
        return msgspec.convert(raw, tp, dec_hook=self.dec_hook)

    def cast(self, value: Any, ty: type | None = None) -> Any:
        """Alias for from_primitive — satisfies KVManager's Caster protocol."""
        return self.from_primitive(value, ty)

    def encode(self, value: Any) -> bytes:
        return msgspec.msgpack.encode(value, enc_hook=self.enc_hook)

    def decode(self, data: bytes) -> Any:
        return self._serializers.rehydrate(msgspec.msgpack.decode(data))
