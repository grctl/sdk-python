import dataclasses
from unittest.mock import MagicMock

import msgspec
import msgspec.msgpack
import pytest
from pydantic import BaseModel

from grctl.worker.codec import CodecRegistry


class Address(BaseModel):
    street: str
    city: str


class User(BaseModel):
    name: str
    age: int
    addresses: list[Address] = []


@dataclasses.dataclass
class Point:
    x: float
    y: float


class _Unregistered:
    pass


def test_pydantic_round_trip():
    registry = CodecRegistry()
    user = User(name="Alice", age=30)

    encoded = registry.encode(user)
    result = msgspec.convert(registry.decode(encoded), User, dec_hook=registry.dec_hook)

    assert result == user


def test_nested_pydantic_round_trip():
    registry = CodecRegistry()
    user = User(name="Bob", age=25, addresses=[Address(street="123 Main St", city="Springfield")])

    encoded = registry.encode(user)
    result = msgspec.convert(registry.decode(encoded), User, dec_hook=registry.dec_hook)

    assert result == user
    assert result.addresses[0].city == "Springfield"


def test_dataclass_round_trip():
    # dataclasses are handled natively by msgspec — enc_hook is never called
    registry = CodecRegistry()
    enc_hook_mock = MagicMock(side_effect=registry.enc_hook)

    point = Point(x=1.5, y=2.5)
    encoded = msgspec.msgpack.encode(point, enc_hook=enc_hook_mock)
    result = msgspec.convert(msgspec.msgpack.decode(encoded), Point)

    assert result == point
    enc_hook_mock.assert_not_called()


def test_primitives_passthrough():
    registry = CodecRegistry()
    enc_hook_mock = MagicMock(side_effect=registry.enc_hook)

    values = [42, "hello", [1, 2, 3], {"key": "value"}, 3.14, True, None]
    for value in values:
        encoded = msgspec.msgpack.encode(value, enc_hook=enc_hook_mock)
        assert msgspec.msgpack.decode(encoded) == value

    enc_hook_mock.assert_not_called()


def test_custom_handler_lifo_priority():
    registry = CodecRegistry()

    # Register a custom handler for User that produces a marker dict
    registry.register(
        lambda tp: issubclass(tp, User),
        lambda obj: {"_custom": True, "name": obj.name},
        lambda tp, data: tp(name=data["name"], age=0),
    )

    user = User(name="Carol", age=40)
    encoded = registry.encode(user)
    decoded_raw = registry.decode(encoded)

    # Custom encoder was used — marker key present
    assert decoded_raw == {"_custom": True, "name": "Carol"}

    result = msgspec.convert(decoded_raw, User, dec_hook=registry.dec_hook)
    assert result.name == "Carol"
    assert result.age == 0  # custom decoder sets age=0


def test_encode_unknown_type_raises():
    registry = CodecRegistry()

    with pytest.raises(TypeError, match="Unsupported type"):
        registry.encode(_Unregistered())


def test_dec_hook_unknown_type_raises():
    registry = CodecRegistry()

    with pytest.raises(TypeError, match="Unsupported type"):
        registry.dec_hook(_Unregistered, {"some": "data"})


class Vector(msgspec.Struct):
    x: float
    y: float


def test_to_primitive_pydantic():
    registry = CodecRegistry()
    user = User(name="Alice", age=30, addresses=[Address(street="1 Main", city="Town")])

    result = registry.to_primitive(user)

    assert result == {"name": "Alice", "age": 30, "addresses": [{"street": "1 Main", "city": "Town"}]}


def test_to_primitive_struct():
    registry = CodecRegistry()
    vec = Vector(x=1.0, y=2.0)

    result = registry.to_primitive(vec)

    assert result == {"x": 1.0, "y": 2.0}


def test_to_primitive_dataclass():
    registry = CodecRegistry()
    point = Point(x=3.0, y=4.0)

    result = registry.to_primitive(point)

    assert result == {"x": 3.0, "y": 4.0}


def test_to_primitive_primitives_passthrough():
    registry = CodecRegistry()

    assert registry.to_primitive(42) == 42
    assert registry.to_primitive("hello") == "hello"
    assert registry.to_primitive([1, 2, 3]) == [1, 2, 3]
    assert registry.to_primitive({"k": "v"}) == {"k": "v"}
    assert registry.to_primitive(None) is None


def test_from_primitive_pydantic():
    registry = CodecRegistry()
    raw = {"name": "Bob", "age": 25, "addresses": []}

    result = registry.from_primitive(raw, User)

    assert isinstance(result, User)
    assert result.name == "Bob"
    assert result.age == 25


def test_from_primitive_struct():
    registry = CodecRegistry()
    raw = {"x": 5.0, "y": 6.0}

    result = registry.from_primitive(raw, Vector)

    assert isinstance(result, Vector)
    assert result.x == 5.0
    assert result.y == 6.0


def test_from_primitive_dataclass():
    registry = CodecRegistry()
    raw = {"x": 7.0, "y": 8.0}

    result = registry.from_primitive(raw, Point)

    assert isinstance(result, Point)
    assert result.x == 7.0
    assert result.y == 8.0
