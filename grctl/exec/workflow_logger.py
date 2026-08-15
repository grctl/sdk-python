"""Logging for code written inside a workflow step.

A step is executed more than once. When the worker running it dies, the server
re-delivers the step and the journal hands back the outcomes already in history
instead of performing them again — but the plain Python statements around those
calls, log statements included, do run again. An operator reading the run back
would see the same line a second time and conclude the work happened twice.

`WorkflowLogger` follows the journal instead: it stays silent while the step is
reproducing its recorded prefix and speaks from the moment the step resumes doing
new work. Only logs written by the workflow author go through it. SDK, worker,
transport, replay-divergence, and failure diagnostics keep their own module
loggers and are never suppressed — replay is exactly when an operator needs them.

Suppression is decided per call, never once per attempt, so a step that replays
three recorded calls and then performs a fourth logs everything from the fourth
onwards.

This is observability, not durability: a line logged after the last recorded
operation is emitted again if the worker dies before the step records anything
further, because nothing durable proves the line was ever reached. Logs create no
history and take no part in replay matching.
"""

import logging
from collections.abc import Mapping
from typing import Any, Protocol

from grctl.logging_config import get_logger
from grctl.models import RunInfo

# The caller sits two frames above `_emit`: the level method, then `_emit` itself.
# Without this the formatter reports this module's line numbers for every workflow log.
_CALLER_DEPTH = 2

_WORKFLOW_LOGGER_NAME = "grctl.workflow"


class ReplayState(Protocol):
    """Whether the step is reproducing recorded history rather than doing new work."""

    @property
    def is_replaying(self) -> bool: ...


class WorkflowLogger:
    """The `logging.Logger` surface, muted for as long as the step is replaying.

    Metadata identifying the run is attached to every record, so a line can be traced
    back to the workflow, run, step, and worker that produced it.
    """

    def __init__(self, logger: logging.Logger, replay_state: ReplayState, metadata: Mapping[str, str]) -> None:
        self._logger = logger
        self._replay_state = replay_state
        self._metadata = dict(metadata)

    def debug(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._emit(logging.DEBUG, msg, args, kwargs)

    def info(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._emit(logging.INFO, msg, args, kwargs)

    def warning(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._emit(logging.WARNING, msg, args, kwargs)

    def error(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._emit(logging.ERROR, msg, args, kwargs)

    def critical(self, msg: object, *args: object, **kwargs: Any) -> None:
        self._emit(logging.CRITICAL, msg, args, kwargs)

    def exception(self, msg: object, *args: object, **kwargs: Any) -> None:
        kwargs.setdefault("exc_info", True)
        self._emit(logging.ERROR, msg, args, kwargs)

    def log(self, level: int, msg: object, *args: object, **kwargs: Any) -> None:
        self._emit(level, msg, args, kwargs)

    def _emit(self, level: int, msg: object, args: tuple[object, ...], kwargs: dict[str, Any]) -> None:
        if self._replay_state.is_replaying:
            return
        extra = {**self._metadata, **(kwargs.pop("extra", None) or {})}
        stacklevel = kwargs.pop("stacklevel", 1) + _CALLER_DEPTH
        self._logger.log(level, msg, *args, extra=extra, stacklevel=stacklevel, **kwargs)


def build_workflow_logger(
    replay_state: ReplayState,
    run_info: RunInfo,
    worker_id: str,
    step_name: str,
) -> WorkflowLogger:
    """Build the logger a step handler writes through, tagged with the run it belongs to.

    All workflow logs share one logger name, so an operator can raise or lower the level
    of user workflow output without touching the SDK's own diagnostics.
    """
    return WorkflowLogger(
        get_logger(_WORKFLOW_LOGGER_NAME),
        replay_state,
        {
            "workflow_id": run_info.wf_id,
            "run_id": run_info.id,
            "workflow_type": run_info.wf_type,
            "step_name": step_name,
            "worker_id": worker_id,
        },
    )
