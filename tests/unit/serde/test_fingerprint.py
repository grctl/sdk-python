"""Stable digest guarantees used to identify replayed operation arguments."""

from datetime import UTC, datetime

from grctl.serde import fingerprint


class TestReplayArgumentFingerprints:
    def test_equal_primitive_values_have_the_same_digest(self) -> None:
        assert fingerprint({"a": 1, "b": "two"}) == fingerprint({"a": 1, "b": "two"})

    def test_mapping_construction_order_does_not_change_the_digest(self) -> None:
        assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})

    def test_nested_mapping_construction_order_does_not_change_the_digest(self) -> None:
        first = {"outer": {"inner": {"x": 1, "y": 2}, "list": [{"p": 1, "q": 2}]}}
        second = {"outer": {"list": [{"q": 2, "p": 1}], "inner": {"y": 2, "x": 1}}}

        assert fingerprint(first) == fingerprint(second)

    def test_different_primitive_values_have_different_digests(self) -> None:
        assert fingerprint({"value": "hello"}) != fingerprint({"value": "world"})

    def test_sequence_order_remains_part_of_the_value(self) -> None:
        assert fingerprint([1, 2, 3]) != fingerprint([3, 2, 1])

    def test_msgpack_distinguishes_primitive_types(self) -> None:
        assert fingerprint({"n": 1}) != fingerprint({"n": "1"})
        assert fingerprint({"n": None}) != fingerprint({"n": 0})

    def test_msgpack_native_datetime_values_have_stable_digests(self) -> None:
        moment = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)

        assert fingerprint({"at": moment}) == fingerprint({"at": moment})
        assert fingerprint({"at": moment}) != fingerprint({"at": datetime(2026, 8, 6, 12, 1, tzinfo=UTC)})

    def test_empty_and_absent_values_are_distinguishable(self) -> None:
        assert fingerprint({}) != fingerprint({"a": None})
