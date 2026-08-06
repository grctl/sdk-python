"""Stability of the value digest replay compares operation arguments by.

The property under test is not "these two strings happen to match" but "a value digests
the same everywhere and forever" — a digest that varies with how a dict was built would
make replay report non-determinism that never happened.
"""

from datetime import UTC, datetime

from grctl.serde import fingerprint


def test_equal_values_digest_equally() -> None:
    assert fingerprint({"a": 1, "b": "two"}) == fingerprint({"a": 1, "b": "two"})


def test_digest_is_independent_of_key_order() -> None:
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})


def test_digest_is_independent_of_nested_key_order() -> None:
    """Where main's msgpack encoding of the args dict got it wrong."""
    deep = {"outer": {"inner": {"x": 1, "y": 2}, "list": [{"p": 1, "q": 2}]}}
    reordered = {"outer": {"list": [{"q": 2, "p": 1}], "inner": {"y": 2, "x": 1}}}

    assert fingerprint(deep) == fingerprint(reordered)


def test_different_values_digest_differently() -> None:
    assert fingerprint({"value": "hello"}) != fingerprint({"value": "world"})


def test_list_order_is_significant() -> None:
    """Unlike mapping keys: the order of a sequence is part of the value."""
    assert fingerprint([1, 2, 3]) != fingerprint([3, 2, 1])


def test_types_are_distinguished() -> None:
    assert fingerprint({"n": 1}) != fingerprint({"n": "1"})
    assert fingerprint({"n": None}) != fingerprint({"n": 0})


def test_digests_primitives_that_reach_history() -> None:
    """The primitive forms a codec actually produces, including msgpack-native datetimes."""
    moment = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)

    assert fingerprint({"at": moment}) == fingerprint({"at": moment})
    assert fingerprint({"at": moment}) != fingerprint({"at": datetime(2026, 8, 6, 12, 1, tzinfo=UTC)})


def test_empty_and_absent_are_distinguishable() -> None:
    assert fingerprint({}) != fingerprint({"a": None})
