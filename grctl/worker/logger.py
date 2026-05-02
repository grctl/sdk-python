import logging
from typing import Any

from grctl.worker.runtime import get_step_runtime


def _is_replaying() -> bool:
    try:
        return get_step_runtime().is_replaying
    except LookupError:
        return False


class ReplayAwareLogger:
    def __init__(self, wf_type: str, logger: logging.Logger | None = None) -> None:
        self._logger = logger if logger is not None else logging.getLogger(f"grctl.workflow.{wf_type}")

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not _is_replaying():
            self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not _is_replaying():
            self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not _is_replaying():
            self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not _is_replaying():
            self._logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not _is_replaying():
            self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not _is_replaying():
            self._logger.exception(msg, *args, **kwargs)
