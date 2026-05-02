from __future__ import annotations

from msgspec import Struct


class ErrorDetails(Struct):
    type: str
    message: str
    stack_trace: str
    qualified_type: str = ""
