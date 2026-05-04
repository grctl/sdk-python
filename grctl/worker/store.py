from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, overload

import msgspec
import msgspec.msgpack

from grctl.worker.codec import CodecRegistry

T = TypeVar("T")


class StoreKeyNotFoundError(KeyError):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key

    def __str__(self) -> str:
        return f"Store key not found: '{self.key}'"


class Store:
    def __init__(
        self,
        loader: Callable[[str], Awaitable[bytes | None]],
        codec: CodecRegistry,
    ) -> None:
        self._data: dict[str, bytes] = {}
        self._dirty: set[str] = set()
        self._loader = loader
        self._codec = codec

    @overload
    async def get(self, key: str) -> Any: ...

    @overload
    async def get(self, key: str, ty: type[T]) -> T: ...

    async def get(self, key: str, ty: type[T] | None = None) -> T | Any:
        raw = self._data.get(key)

        if raw is None:
            raw = await self._loader(key)
            if raw is None:
                raise StoreKeyNotFoundError(key)
            self._data[key] = raw

        decoded = msgspec.msgpack.decode(raw)

        if ty is None:
            return decoded

        return msgspec.convert(decoded, ty, dec_hook=self._codec.dec_hook)

    def put(self, key: str, value: Any) -> None:
        # Encode immediately — surfaces serialization errors before NATS flush
        self._data[key] = self._codec.encode(value)
        self._dirty.add(key)

    def get_pending_updates(self) -> dict[str, Any] | None:
        if not self._dirty:
            return None

        result = {k: msgspec.msgpack.decode(self._data[k]) for k in self._dirty}
        self._dirty.clear()
        return result
