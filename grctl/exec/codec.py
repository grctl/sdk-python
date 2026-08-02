"""The value-conversion port the execution layer depends on.

Defined here rather than alongside any one consumer: an Execution, its Context
and its Tasks all convert values, and all three must be handed the *same*
converter so a type a user registered a serialiser for behaves identically
whether it appears in a handler parameter, a task argument or a task result.
"""

from typing import Any, Protocol


class Codec(Protocol):
    def from_primitive(self, raw: Any, tp: type | None = None) -> Any: ...

    def to_primitive(self, value: Any) -> Any: ...
