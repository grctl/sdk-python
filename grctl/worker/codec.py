from collections.abc import Callable
from typing import Any

import msgspec
import msgspec.msgpack
from pydantic import BaseModel

type CheckFn = Callable[[type], bool]
type EncodeFn = Callable[[Any], Any]
type DecodeFn = Callable[[type, Any], Any]


class CodecRegistry:
    def __init__(self) -> None:
        self._handlers: list[tuple[CheckFn, EncodeFn, DecodeFn]] = [
            (
                lambda tp: issubclass(tp, BaseModel),
                lambda obj: obj.model_dump(),
                lambda tp, data: tp.model_validate(data),
            )
        ]

    def register(self, check: CheckFn, encode: EncodeFn, decode: DecodeFn) -> None:
        # LIFO — last registered wins over earlier handlers
        self._handlers.insert(0, (check, encode, decode))

    def enc_hook(self, obj: Any) -> Any:
        for check, encode, _ in self._handlers:
            if check(type(obj)):
                return encode(obj)
        raise TypeError(f"Unsupported type: {type(obj)}")

    def dec_hook(self, tp: type, obj: Any) -> Any:
        for check, _, decode in self._handlers:
            if check(tp):
                return decode(tp, obj)
        raise TypeError(f"Unsupported type: {tp}")

    def to_primitive(self, value: Any) -> Any:
        return msgspec.to_builtins(value, enc_hook=self.enc_hook)

    def from_primitive(self, raw: Any, tp: type) -> Any:
        return msgspec.convert(raw, tp, dec_hook=self.dec_hook)

    def encode(self, value: Any) -> bytes:
        return msgspec.msgpack.encode(value, enc_hook=self.enc_hook)

    def decode(self, data: bytes) -> Any:
        return msgspec.msgpack.decode(data)
