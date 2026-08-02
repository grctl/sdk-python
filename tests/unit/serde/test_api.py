from decimal import Decimal
from typing import Any

import pytest

from grctl.serde import SerializerRegistry, register, serializer
from tests.unit.serde.fakes import Money


@pytest.fixture
def registry() -> SerializerRegistry:
    return SerializerRegistry()


def test_decorator_registers_the_class_it_decorates(registry: SerializerRegistry) -> None:
    @serializer(Money, registry=registry)
    class MoneySerializer:
        def encode(self, value: Money) -> Any:
            return str(value.amount)

        def decode(self, raw: Any) -> Money:
            return Money(Decimal(raw), "EUR")

    assert registry.decode(Money, registry.encode(Money(Decimal(5), "EUR"))) == Money(Decimal(5), "EUR")


def test_decorator_returns_the_class_unchanged(registry: SerializerRegistry) -> None:
    @serializer(Money, registry=registry)
    class MoneySerializer:
        def encode(self, value: Money) -> Any:
            return None

        def decode(self, raw: Any) -> Money:
            return Money(Decimal(0), "EUR")

    assert isinstance(MoneySerializer(), MoneySerializer)


def test_decorator_rejects_a_class_that_is_not_a_serializer(registry: SerializerRegistry) -> None:
    with pytest.raises(TypeError):

        @serializer(Money, registry=registry)
        class Incomplete:
            def encode(self, value: Money) -> Any:
                return None


def test_register_accepts_an_already_constructed_serializer(registry: SerializerRegistry) -> None:
    class MoneySerializer:
        def encode(self, value: Money) -> Any:
            return str(value.amount)

        def decode(self, raw: Any) -> Money:
            return Money(Decimal(raw), "EUR")

    register(Money, MoneySerializer(), registry=registry)

    assert registry.encode(Money(Decimal(5), "EUR"))["$val"] == "5"
