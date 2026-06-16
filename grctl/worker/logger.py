import logging
from collections.abc import Callable


class ReplayFilter(logging.Filter):
    def __init__(self, is_replaying: Callable[[], bool]) -> None:
        super().__init__()
        self._is_replaying = is_replaying

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: ARG002
        try:
            return not self._is_replaying()
        except Exception:
            return True
