from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar, overload

T = TypeVar("T")


class Loader(Protocol):
    async def load(self, key: str, ty: type[T] | None = None) -> T | Any: ...


class Caster(Protocol):
    def cast(self, value: Any, ty: type[T]) -> T: ...


@dataclass(slots=True)
class PendingKVUpdates:
    sets: dict[str, Any] = field(default_factory=dict)
    deletes: set[str] = field(default_factory=set)

    def is_empty(self) -> bool:
        return not self.sets and not self.deletes


class StoreKeyNotFoundError(KeyError):
    def __init__(self, key: str) -> None:
        super().__init__(key)
        self.key = key

    def __str__(self) -> str:
        return f"Store key not found: '{self.key}'"


class KVManager:
    def __init__(
        self,
        loader: Loader,
        caster: Caster,
    ) -> None:
        self.data: dict[str, Any] = {}
        self.deleted: set[str] = set()
        self.pending = PendingKVUpdates()
        self.loader = loader
        self.caster = caster

    @overload
    async def get(self, key: str) -> Any: ...

    @overload
    async def get(self, key: str, ty: type[T]) -> T: ...

    async def get(self, key: str, ty: type[T] | None = None) -> T | Any:
        if key in self.deleted:
            raise StoreKeyNotFoundError(key)

        if key not in self.data:
            if ty is None:
                val = await self.loader.load(key)
            else:
                val = await self.loader.load(key, ty)

            if val is None:
                raise StoreKeyNotFoundError(key)

            self.data[key] = val

        val = self.data[key]

        if ty is None:
            return val

        if isinstance(val, ty):
            return val

        typed = self.caster.cast(val, ty)
        self.data[key] = typed
        return typed

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.deleted.discard(key)
        self.pending.deletes.discard(key)
        self.pending.sets[key] = value

    def delete(self, key: str) -> None:
        self.data.pop(key, None)
        self.deleted.add(key)
        self.pending.sets.pop(key, None)
        self.pending.deletes.add(key)

    def get_pending_updates(self) -> dict[str, Any] | None:
        if not self.pending.sets:
            return None
        return dict(self.pending.sets)

    # TODO: We should use this when we update the server to process both sets and deletes
    def get_pending_updates_with_deletes(self) -> PendingKVUpdates | None:
        if self.pending.is_empty():
            return None

        result = PendingKVUpdates(sets=dict(self.pending.sets), deletes=set(self.pending.deletes))
        self.pending = PendingKVUpdates()
        return result
