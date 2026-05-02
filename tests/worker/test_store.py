import msgspec
import pytest
from pydantic import BaseModel

from grctl.worker.codec import CodecRegistry
from grctl.worker.store import Store, StoreKeyNotFoundError


class Item(msgspec.Struct):
    name: str
    value: int


class Product(BaseModel):
    title: str
    price: float


@pytest.fixture
def registry():
    return CodecRegistry()


@pytest.fixture
def store(registry):
    async def null_loader(key: str) -> bytes | None:
        return None

    return Store(loader=null_loader, codec=registry)


async def test_put_untyped_get_primitives(store):
    store.put("str_key", "hello")
    store.put("int_key", 42)
    store.put("list_key", [1, 2, 3])
    store.put("dict_key", {"a": 1})

    assert await store.get("str_key") == "hello"
    assert await store.get("int_key") == 42
    assert await store.get("list_key") == [1, 2, 3]
    assert await store.get("dict_key") == {"a": 1}


async def test_put_typed_get_struct(store):
    item = Item(name="widget", value=10)
    store.put("item", item)

    result = await store.get("item", Item)

    assert result == item


async def test_put_dict_typed_get_struct(store):
    store.put("item", {"name": "gadget", "value": 5})

    result = await store.get("item", Item)

    assert isinstance(result, Item)
    assert result.name == "gadget"
    assert result.value == 5


async def test_put_pydantic_typed_get_returns_model(store):
    product = Product(title="Widget", price=9.99)
    store.put("product", product)

    result = await store.get("product", Product)

    assert isinstance(result, Product)
    assert result == product


async def test_put_pydantic_untyped_get_returns_dict(store):
    product = Product(title="Widget", price=9.99)
    store.put("product", product)

    result = await store.get("product")

    assert isinstance(result, dict)
    assert result["title"] == "Widget"
    assert result["price"] == pytest.approx(9.99)


async def test_typed_get_missing_key_raises(store):
    with pytest.raises(StoreKeyNotFoundError, match="missing"):
        await store.get("missing", Item)


async def test_untyped_get_missing_key_raises(store):
    with pytest.raises(StoreKeyNotFoundError, match="missing"):
        await store.get("missing")


async def test_typed_get_wrong_type_raises(store):
    store.put("item", {"name": "widget", "value": "not_an_int"})

    with pytest.raises(msgspec.ValidationError):
        await store.get("item", Item)


async def test_put_non_serializable_raises(store):
    class Unregistered:
        pass

    with pytest.raises(TypeError, match="Unsupported type"):
        store.put("key", Unregistered())


async def test_get_pending_updates_returns_decoded_values(store):
    store.put("a", 1)
    store.put("b", "hello")

    pending = store.get_pending_updates()

    assert pending is not None
    assert pending == {"a": 1, "b": "hello"}


async def test_get_pending_updates_clears_dirty(store):
    store.put("key", 42)

    store.get_pending_updates()
    second = store.get_pending_updates()

    assert second is None


async def test_loader_sourced_value_typed_get(registry):
    stored_bytes = registry.encode(Item(name="loaded", value=99))

    async def loader(key: str) -> bytes | None:
        return stored_bytes if key == "item" else None

    s = Store(loader=loader, codec=registry)
    result = await s.get("item", Item)

    assert isinstance(result, Item)
    assert result.name == "loaded"
    assert result.value == 99
