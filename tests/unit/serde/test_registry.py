from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, NamedTuple
from uuid import UUID

import msgspec
import pytest
from pydantic import BaseModel

from grctl.serde import NativeTypeError, SerializerConflictError, SerializerRegistry, UnsupportedTypeError
from tests.unit.serde.fakes import Money, MoneySerializer


@dataclass
class DataclassPayload:
    value: int


class EnumPayload(Enum):
    ACCEPTED = "accepted"


class NamedTuplePayload(NamedTuple):
    value: int


class MsgspecPayload(msgspec.Struct):
    value: int


@pytest.fixture
def registry() -> SerializerRegistry:
    result = SerializerRegistry()
    result.register(Money, MoneySerializer())
    return result


class TestRegisteredTypes:
    def test_registered_value_converts_to_its_serializer_output(self, registry: SerializerRegistry) -> None:
        assert registry.encode(Money(Decimal("9.99"), "EUR")) == {"amount": "9.99", "currency": "EUR"}

    def test_registered_value_round_trips_when_the_caller_requests_its_type(self, registry: SerializerRegistry) -> None:
        original = Money(Decimal("9.99"), "EUR")

        assert registry.decode(Money, registry.encode(original)) == original

    def test_raw_primitives_can_be_cast_to_a_registered_type(self, registry: SerializerRegistry) -> None:
        assert registry.decode(Money, {"amount": "1", "currency": "EUR"}) == Money(Decimal(1), "EUR")


class TestRegistrationRules:
    def test_unregistered_value_names_its_unsupported_type(self, registry: SerializerRegistry) -> None:
        class Ghost: ...

        with pytest.raises(UnsupportedTypeError) as error:
            registry.encode(Ghost())

        assert error.value.type is Ghost

    def test_conflicting_type_registration_is_rejected_before_writing_history(
        self, registry: SerializerRegistry
    ) -> None:
        with pytest.raises(SerializerConflictError):
            registry.register(Money, MoneySerializer())

    def test_override_replaces_an_existing_serializer(self, registry: SerializerRegistry) -> None:
        class EuroMoneySerializer:
            def encode(self, value: Money) -> Any:
                return str(value.amount)

            def decode(self, raw: Any) -> Money:
                return Money(Decimal(raw), "EUR")

        registry.register(Money, EuroMoneySerializer(), override=True)

        assert registry.encode(Money(Decimal(1), "USD")) == "1"

    @pytest.mark.parametrize(
        "native_type",
        [DataclassPayload, EnumPayload, NamedTuplePayload, MsgspecPayload, Decimal, UUID, datetime],
    )
    def test_registration_for_a_wire_native_type_is_rejected(self, native_type: type) -> None:
        with pytest.raises(NativeTypeError):
            SerializerRegistry().register(native_type, MoneySerializer())

    def test_invalid_direct_registration_is_rejected_before_the_first_encode(self) -> None:
        class IncompleteSerializer:
            def encode(self, value: Money) -> Any:
                return str(value.amount)

        with pytest.raises(TypeError):
            SerializerRegistry().register(Money, IncompleteSerializer())  # ty:ignore[invalid-argument-type]


class TestBuiltInFamilies:
    def test_pydantic_model_round_trips_without_manual_registration(self) -> None:
        class Order(BaseModel):
            sku: str

        registry = SerializerRegistry()

        assert registry.decode(Order, registry.encode(Order(sku="abc"))) == Order(sku="abc")

    def test_pydantic_model_preserves_nested_model_types(self) -> None:
        class Address(BaseModel):
            city: str

        class Order(BaseModel):
            address: Address

        registry = SerializerRegistry()
        original = Order(address=Address(city="London"))

        decoded = registry.decode(Order, registry.encode(original))

        assert decoded == original
        assert isinstance(decoded.address, Address)

    def test_registry_can_exclude_builtin_type_families(self) -> None:
        class Order(BaseModel):
            sku: str

        with pytest.raises(UnsupportedTypeError):
            SerializerRegistry(include_builtins=False).encode(Order(sku="abc"))

    def test_non_class_annotation_does_not_break_predicate_lookup(self, registry: SerializerRegistry) -> None:
        with pytest.raises(UnsupportedTypeError):
            registry.decode(list[int], {"amount": "1", "currency": "EUR"})
