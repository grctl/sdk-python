from dataclasses import dataclass
from decimal import Decimal
from typing import Any

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


@pytest.fixture
def registry() -> SerializerRegistry:
    reg = SerializerRegistry()
    reg.register(Money, MoneySerializer())
    return reg


def test_encoded_value_carries_its_tag_and_version(registry: SerializerRegistry) -> None:
    encoded = registry.encode(Money(Decimal("9.99"), "EUR"))

    assert encoded == {"$type": "Money", "$ver": 1, "$val": {"amount": "9.99", "currency": "EUR"}}


def test_round_trips_through_its_serializer(registry: SerializerRegistry) -> None:
    original = Money(Decimal("9.99"), "EUR")

    assert registry.decode(Money, registry.encode(original)) == original


def test_explicit_tag_survives_a_class_rename(registry: SerializerRegistry) -> None:
    registry.register(Invoice, InvoiceSerializer(), tag="invoice.v1")

    assert registry.encode(Invoice(Money(Decimal(1), "EUR")))["$type"] == "invoice.v1"


def test_nested_registered_types_are_rebuilt(registry: SerializerRegistry) -> None:
    registry.register(Invoice, InvoiceSerializer())
    original = Invoice(Money(Decimal("42.00"), "GBP"))

    assert registry.decode(Invoice, registry.encode(original)) == original


def test_decoding_into_the_wrong_type_is_rejected(registry: SerializerRegistry) -> None:
    registry.register(Invoice, InvoiceSerializer())
    encoded = registry.encode(Money(Decimal(1), "EUR"))

    with pytest.raises(TagMismatchError):
        registry.decode(Invoice, encoded)


def test_decoding_an_untagged_value_is_rejected(registry: SerializerRegistry) -> None:
    with pytest.raises(MalformedEnvelopeError):
        registry.decode(Money, {"amount": "1", "currency": "EUR"})


def test_unregistered_type_reports_itself(registry: SerializerRegistry) -> None:
    class Ghost:
        pass

    with pytest.raises(UnsupportedTypeError):
        registry.encode(Ghost())


def test_pydantic_models_are_supported_out_of_the_box() -> None:
    class Order(BaseModel):
        sku: str

    registry = SerializerRegistry()

    assert registry.decode(Order, registry.encode(Order(sku="abc"))) == Order(sku="abc")


def test_builtins_can_be_left_out() -> None:
    class Order(BaseModel):
        sku: str

    registry = SerializerRegistry(include_builtins=False)

    with pytest.raises(UnsupportedTypeError):
        registry.encode(Order(sku="abc"))


def test_registering_a_type_twice_is_rejected(registry: SerializerRegistry) -> None:
    with pytest.raises(SerializerConflictError):
        registry.register(Money, MoneySerializer())


def test_registering_a_type_twice_is_allowed_when_overriding(registry: SerializerRegistry) -> None:
    registry.register(Money, MoneySerializer(), tag="money.v2", override=True)

    assert registry.encode(Money(Decimal(1), "EUR"))["$type"] == "money.v2"


def test_two_types_cannot_share_a_tag(registry: SerializerRegistry) -> None:
    with pytest.raises(SerializerConflictError):
        registry.register(Invoice, InvoiceSerializer(), tag="Money")


def test_serializers_for_natively_encoded_types_are_rejected() -> None:
    @dataclass
    class Point:
        x: int

    with pytest.raises(NativeTypeError):
        SerializerRegistry().register(Point, MoneySerializer())


def test_an_exact_serializer_wins_over_a_predicate_rule() -> None:
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


def test_lookup_ignores_types_issubclass_cannot_handle(registry: SerializerRegistry) -> None:
    """Annotations reach the registry as arbitrary typing constructs, not just classes."""
    with pytest.raises(UnsupportedTypeError):
        registry.decode(list[int], {"$type": "Money", "$ver": 1, "$val": {}})


class TemperatureV2Serializer:
    def encode(self, value: Money) -> Any:
        return {"celsius": str(value.amount)}

    def decode(self, raw: Any) -> Money:
        return Money(Decimal(raw["celsius"]), "C")

    def migrate(self, raw: Any, from_version: int) -> Any:
        return {"celsius": raw["amount"]}


def test_older_values_are_migrated_when_the_serializer_can(registry: SerializerRegistry) -> None:
    written_at_v1 = registry.encode(Money(Decimal(20), "C"))

    upgraded = SerializerRegistry()
    upgraded.register(Money, TemperatureV2Serializer(), version=2)

    assert upgraded.decode(Money, written_at_v1) == Money(Decimal(20), "C")


def test_older_values_are_rejected_when_the_serializer_cannot_migrate(registry: SerializerRegistry) -> None:
    written_at_v1 = registry.encode(Money(Decimal(20), "C"))

    upgraded = SerializerRegistry()
    upgraded.register(Money, MoneySerializer(), version=2)

    with pytest.raises(VersionMismatchError):
        upgraded.decode(Money, written_at_v1)


def test_rehydrate_rebuilds_known_tags_inside_containers(registry: SerializerRegistry) -> None:
    encoded = {"prices": [registry.encode(Money(Decimal(1), "EUR"))], "count": 1}

    assert registry.rehydrate(encoded) == {"prices": [Money(Decimal(1), "EUR")], "count": 1}


def test_rehydrate_degrades_unknown_tags_to_their_payload() -> None:
    registry = SerializerRegistry()

    assert registry.rehydrate({"$type": "Gone", "$ver": 1, "$val": {"x": 1}}) == {"x": 1}
