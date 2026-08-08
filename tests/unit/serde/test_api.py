"""Behavioural guarantees for the public serialiser registration API."""

from decimal import Decimal
from typing import Any

import pytest

from grctl.serde import SerializerRegistry, register, serializer
from tests.unit.serde.fakes import Money


@pytest.fixture
def registry() -> SerializerRegistry:
    return SerializerRegistry()


class TestDecoratorRegistration:
    def test_decorated_serializer_registers_the_user_type_at_definition_time(
        self, registry: SerializerRegistry
    ) -> None:
        @serializer(Money, registry=registry)
        class MoneySerializer:
            def encode(self, value: Money) -> Any:
                return str(value.amount)

            def decode(self, raw: Any) -> Money:
                return Money(Decimal(raw), "EUR")

        assert registry.decode(Money, registry.encode(Money(Decimal(5), "EUR"))) == Money(Decimal(5), "EUR")

    def test_decorator_preserves_the_serializer_class_for_user_code(self, registry: SerializerRegistry) -> None:
        @serializer(Money, registry=registry)
        class MoneySerializer:
            def encode(self, value: Money) -> Any:
                return None

            def decode(self, raw: Any) -> Money:
                return Money(Decimal(0), "EUR")

        assert isinstance(MoneySerializer(), MoneySerializer)

    def test_decorator_rejects_an_incomplete_serializer_before_registration(self, registry: SerializerRegistry) -> None:
        with pytest.raises(TypeError):

            @serializer(Money, registry=registry)
            class Incomplete:
                def encode(self, value: Money) -> Any:
                    return None


class TestDirectRegistration:
    def test_constructed_serializer_can_be_registered_without_the_decorator(self, registry: SerializerRegistry) -> None:
        class MoneySerializer:
            def encode(self, value: Money) -> Any:
                return str(value.amount)

            def decode(self, raw: Any) -> Money:
                return Money(Decimal(raw), "EUR")

        register(Money, MoneySerializer(), registry=registry)

        assert registry.encode(Money(Decimal(5), "EUR"))["$val"] == "5"
