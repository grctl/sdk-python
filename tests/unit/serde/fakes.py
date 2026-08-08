"""User types that require a serialiser because the wire codec does not know them."""

from decimal import Decimal
from typing import Any


class Money:
    def __init__(self, amount: Decimal, currency: str) -> None:
        self.amount = amount
        self.currency = currency

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Money) and (self.amount, self.currency) == (other.amount, other.currency)

    def __hash__(self) -> int:
        return hash((self.amount, self.currency))

    def __repr__(self) -> str:
        return f"Money({self.amount}, {self.currency!r})"


class MoneySerializer:
    def encode(self, value: Money) -> Any:
        return {"amount": str(value.amount), "currency": value.currency}

    def decode(self, raw: Any) -> Money:
        return Money(Decimal(raw["amount"]), raw["currency"])


class Invoice:
    def __init__(self, total: Money) -> None:
        self.total = total

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Invoice) and self.total == other.total

    def __hash__(self) -> int:
        return hash(self.total)


class InvoiceSerializer:
    def encode(self, value: Invoice) -> Any:
        return {"total": value.total}

    def decode(self, raw: Any) -> Invoice:
        return Invoice(raw["total"])
