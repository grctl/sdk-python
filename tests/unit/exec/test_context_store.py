import pytest

from grctl.exec.kv_manager import StoreKeyNotFoundError
from tests.unit.exec.fakes import make_context


async def test_store_set_and_get_supports_untyped_and_typed_reads() -> None:
    context = make_context()

    context.store.set("answer", 42)

    assert await context.store.get("answer") == 42
    assert await context.store.get("answer", int) == 42


async def test_store_get_raises_for_missing_key() -> None:
    context = make_context()

    with pytest.raises(StoreKeyNotFoundError):
        await context.store.get("missing")


async def test_context_exposes_supplied_store() -> None:
    class StoreDouble:
        async def get(self, key: str, ty: type | None = None) -> object:
            return None

        def set(self, key: str, value: object) -> None:
            return None

    store = StoreDouble()

    context = make_context(store=store)

    assert context.store is store
