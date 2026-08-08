"""Behavioural guarantees for user-type serialiser registration and replay."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, NamedTuple
from uuid import UUID

import msgspec
import pytest
from pydantic import BaseModel

from grctl.serde import (
    MalformedEnvelopeError,
    NativeTypeError,
    SerializerConflictError,
    SerializerRegistry,
    TagMismatchError,
    UnsupportedTypeError,
    VersionMismatchError,
)
from tests.unit.serde.fakes import Invoice, InvoiceSerializer, Money, MoneySerializer


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


class TestDurableUserValues:
    def test_registered_value_records_its_type_tag_version_and_primitive_payload(
        self, registry: SerializerRegistry
    ) -> None:
        encoded = registry.encode(Money(Decimal("9.99"), "EUR"))

        assert encoded == {"$type": "Money", "$ver": 1, "$val": {"amount": "9.99", "currency": "EUR"}}

    def test_registered_value_round_trips_through_durable_history(self, registry: SerializerRegistry) -> None:
        original = Money(Decimal("9.99"), "EUR")

        assert registry.decode(Money, registry.encode(original)) == original

    def test_explicit_tag_keeps_values_readable_after_a_class_rename(self, registry: SerializerRegistry) -> None:
        registry.register(Invoice, InvoiceSerializer(), tag="invoice.v1")

        assert registry.encode(Invoice(Money(Decimal(1), "EUR")))["$type"] == "invoice.v1"

    def test_nested_registered_values_are_rebuilt_before_the_outer_value_decodes(
        self, registry: SerializerRegistry
    ) -> None:
        registry.register(Invoice, InvoiceSerializer())
        original = Invoice(Money(Decimal("42.00"), "GBP"))

        assert registry.decode(Invoice, registry.encode(original)) == original


class TestIncompatibleDurableValues:
    def test_value_written_for_a_different_type_is_rejected(self, registry: SerializerRegistry) -> None:
        registry.register(Invoice, InvoiceSerializer())

        with pytest.raises(TagMismatchError):
            registry.decode(Invoice, registry.encode(Money(Decimal(1), "EUR")))

    def test_value_without_a_serialiser_envelope_is_rejected(self, registry: SerializerRegistry) -> None:
        with pytest.raises(MalformedEnvelopeError):
            registry.decode(Money, {"amount": "1", "currency": "EUR"})

    def test_old_value_without_a_migration_is_rejected(self, registry: SerializerRegistry) -> None:
        written_at_v1 = registry.encode(Money(Decimal(20), "C"))
        upgraded = SerializerRegistry()
        upgraded.register(Money, MoneySerializer(), version=2)

        with pytest.raises(VersionMismatchError):
            upgraded.decode(Money, written_at_v1)

    def test_old_value_is_upgraded_before_decoding_when_a_migration_exists(self, registry: SerializerRegistry) -> None:
        class TemperatureV2Serializer:
            def encode(self, value: Money) -> Any:
                return {"celsius": str(value.amount)}

            def decode(self, raw: Any) -> Money:
                return Money(Decimal(raw["celsius"]), "C")

            def migrate(self, raw: Any, from_version: int) -> Any:
                assert from_version == 1
                return {"celsius": raw["amount"]}

        written_at_v1 = registry.encode(Money(Decimal(20), "C"))
        upgraded = SerializerRegistry()
        upgraded.register(Money, TemperatureV2Serializer(), version=2)

        assert upgraded.decode(Money, written_at_v1) == Money(Decimal(20), "C")


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

    def test_explicit_override_replaces_the_registered_durable_tag(self, registry: SerializerRegistry) -> None:
        registry.register(Money, MoneySerializer(), tag="money.v2", override=True)

        assert registry.encode(Money(Decimal(1), "EUR"))["$type"] == "money.v2"

    def test_two_user_types_cannot_claim_the_same_durable_tag(self, registry: SerializerRegistry) -> None:
        with pytest.raises(SerializerConflictError):
            registry.register(Invoice, InvoiceSerializer(), tag="Money")

    @pytest.mark.parametrize(
        "native_type",
        [DataclassPayload, EnumPayload, NamedTuplePayload, MsgspecPayload, Decimal, UUID, datetime],
    )
    def test_registration_for_a_wire_native_type_is_rejected(self, native_type: type) -> None:
        with pytest.raises(NativeTypeError):
            SerializerRegistry().register(native_type, MoneySerializer())

    def test_exact_registration_takes_precedence_over_a_matching_family_rule(self) -> None:
        class Order(BaseModel):
            sku: str

        class OrderSerializer:
            def encode(self, value: Order) -> Any:
                return value.sku

            def decode(self, raw: Any) -> Order:
                return Order(sku=raw)

        registry = SerializerRegistry()
        registry.register(Order, OrderSerializer())

        assert registry.encode(Order(sku="abc"))["$val"] == "abc"

    def test_non_class_annotation_does_not_break_predicate_lookup(self, registry: SerializerRegistry) -> None:
        with pytest.raises(UnsupportedTypeError):
            registry.decode(list[int], {"$type": "Money", "$ver": 1, "$val": {}})


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


class TestUntypedHistoryReads:
    def test_known_tag_inside_containers_rehydrates_to_the_registered_type(self, registry: SerializerRegistry) -> None:
        encoded = {"prices": [registry.encode(Money(Decimal(1), "EUR"))], "count": 1}

        assert registry.rehydrate(encoded) == {"prices": [Money(Decimal(1), "EUR")], "count": 1}

    def test_unknown_tag_degrades_to_its_payload_for_an_untyped_read(self) -> None:
        assert SerializerRegistry().rehydrate({"$type": "Gone", "$ver": 1, "$val": {"x": 1}}) == {"x": 1}
