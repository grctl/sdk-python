import asyncio
from collections.abc import Callable
from logging import Logger
from typing import Any, Protocol

from grctl.models import (
    ErrorDetails,
    HistoryEvent,
    HistoryKind,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunInfo,
    RunTerminated,
    RunTimeout,
)
from grctl.models.errors import WorkflowError


class HistoryListener(Protocol):
    """Delivers a run's history events to a handler until stopped.

    A listener belongs to the one future that created it and is started exactly once.
    Any number of listeners may observe the same run — two clients watching one workflow
    each get their own — so a listener never coordinates with, or is shared by, another.

    stop() is idempotent and terminal: a future reaches it from its done callback and
    from an explicit stop, and never resumes afterwards.

    On start the listener delivers the run's most recent history event and everything
    after it, which is what lets a future attach to a run that has already finished and
    still settle on its outcome.
    """

    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class HistoryListenerFactory(Protocol):
    """Builds a HistoryListener for a run once a handler exists to bind it to.

    A factory rather than a plain instance because the handler is a bound method on the
    WorkflowFuture itself, which doesn't exist yet at the point the listener is requested.
    """

    def create(self, run_info: RunInfo, handler: Callable[[HistoryEvent], None]) -> HistoryListener: ...


class ResultDecoder(Protocol):
    """Converts a run's raw completion value into a caller-requested type."""

    def from_primitive(self, raw: Any, tp: type | None = None) -> Any: ...


class WorkflowFuture(asyncio.Future[Any]):
    """Future for workflow run with built-in event handling and lifecycle management."""

    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        listener_factory: HistoryListenerFactory,
        logger: Logger,
        payload: Any | None = None,
        return_type: type | None = None,
        decoder: ResultDecoder | None = None,
    ) -> None:
        super().__init__()
        self.run_info = run_info
        self.payload = payload
        self._return_type = return_type
        self._decoder = decoder
        self._listener = listener_factory.create(run_info, self._handle_history_event)
        self.add_done_callback(self._schedule_listener_stop)
        self._history_update_handlers: dict[HistoryKind, Callable[[HistoryEvent], None]] = {
            HistoryKind.run_scheduled: self._on_non_terminal_event,
            HistoryKind.run_started: self._on_non_terminal_event,
            HistoryKind.run_completed: self._on_run_completed,
            HistoryKind.run_failed: self._on_run_failed,
            HistoryKind.run_timeout: self._on_run_timeout,
            HistoryKind.run_cancelled: self._on_run_cancelled,
            HistoryKind.run_terminated: self._on_run_terminated,
        }
        self._logger = logger

    async def start(self) -> None:
        """Start listening for events and publish run command."""
        await self._listener.start()

    def _schedule_listener_stop(self, _: asyncio.Future) -> None:
        # done_callback must be sync, so we schedule the async stop as a task.
        asyncio.ensure_future(self._listener.stop())  # noqa: RUF006

    async def stop(self) -> None:
        """Stop listening for events and cleanup."""
        await self._listener.stop()

        if not self.done():
            self.cancel()

    async def discard(self) -> None:
        """Release a future started in a step that ended without awaiting it.

        Stops the history listener and, if the run already settled, retrieves the
        outcome so asyncio does not warn that the exception was never retrieved. Used
        for child handles that the parent observes via a completion callback instead of
        the future.
        """
        await self.stop()
        if self.done() and not self.cancelled():
            self.exception()  # mark retrieved; value is intentionally ignored

    def _handle_history_event(self, event: HistoryEvent) -> None:
        """Process a history event delivered by the listener."""
        try:
            handler = self._history_update_handlers.get(event.kind)
            if handler is None:
                self._logger.debug(
                    "Workflow %s received history event kind %s",
                    self.run_info.id,
                    event.kind,
                )
                return

            handler(event)

        except Exception as e:
            self._logger.exception("Error handling run event")
            if not self.done():
                self.set_exception(e)

    def _on_non_terminal_event(self, event: HistoryEvent) -> None:
        self._logger.debug(
            "Run %s received non-terminal history event %s",
            self.run_info.id,
            event.kind,
        )

    def _on_run_completed(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunCompleted):
            self._logger.error("Run %s completed event payload mismatch: %s", self.run_info.id, type(payload))
            return
        result = payload.result
        if self._decoder is not None:
            result = self._decoder.from_primitive(result, self._return_type)
        self.set_result(result)

    def _on_run_failed(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunFailed):
            self._logger.error("Run %s failed event payload mismatch: %s", self.run_info.id, type(payload))
            return
        self._logger.debug("Workflow failed with error: %s", payload)
        error_detail = payload.error
        if not isinstance(error_detail, ErrorDetails):
            error_detail = ErrorDetails(**error_detail)

        error_type = error_detail.type if error_detail else "UnknownError"
        error_msg = f"{error_type}: {error_detail.message if error_detail and error_detail.message else 'No message'}"
        self.set_exception(WorkflowError(error_msg))

    def _on_run_timeout(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunTimeout):
            self._logger.error("Run %s timeout payload mismatch: %s", self.run_info.id, type(payload))
            return
        error_msg = f"Workflow timed out after {payload.duration_ms}s"
        self.set_exception(TimeoutError(error_msg))

    def _on_run_cancelled(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunCancelled):
            self._logger.error("Run %s cancel payload mismatch: %s", self.run_info.id, type(payload))
            return
        self.set_exception(asyncio.CancelledError("Workflow cancelled"))

    def _on_run_terminated(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunTerminated):
            self._logger.error("Run %s terminated payload mismatch: %s", self.run_info.id, type(payload))
            return
        self.set_exception(asyncio.CancelledError("Workflow terminated"))
