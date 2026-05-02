import asyncio
import json
from collections.abc import Callable
from typing import Any

import msgspec
from nats.aio.client import Client as NATSClient

from grctl.logging_config import get_logger
from grctl.models import (
    ErrorDetails,
    HistoryEvent,
    HistoryKind,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunInfo,
    RunTimeout,
)
from grctl.models.errors import WorkflowError
from grctl.nats.history_sub import HistorySubscriber

logger = get_logger(__name__)


class WorkflowFuture(asyncio.Future[Any]):
    """Future for workflow run with built-in event handling and lifecycle management."""

    def __init__(
        self,
        run_info: RunInfo,
        nc: NATSClient,
        payload: Any | None = None,
    ) -> None:
        super().__init__()
        self.run_info = run_info
        self.payload = payload
        self._subscriber = HistorySubscriber(
            nc=nc,
            wf_id=run_info.wf_id,
            run_id=run_info.id,
            handler=self._handle_history_event,
        )
        self._history_update_handlers: dict[HistoryKind, Callable[[HistoryEvent], None]] = {
            HistoryKind.run_scheduled: self._on_non_terminal_event,
            HistoryKind.run_started: self._on_non_terminal_event,
            HistoryKind.run_completed: self._on_run_completed,
            HistoryKind.run_failed: self._on_run_failed,
            HistoryKind.run_timeout: self._on_run_timeout,
            HistoryKind.run_cancelled: self._on_run_cancelled,
        }

    async def start(self) -> None:
        """Start listening for events and publish run command."""
        await self._subscriber.start()

    async def stop(self) -> None:
        """Stop listening for events and cleanup."""
        await self._subscriber.stop()

        if not self.done():
            self.cancel()

    def _handle_history_event(self, event: HistoryEvent) -> None:
        """Process a history event from the subscription."""
        try:
            payload = json.dumps(msgspec.to_builtins(event), indent=2, sort_keys=True)
            logger.debug(
                "Run %s received history event %s",
                self.run_info.id,
                payload,
            )
            handler = self._history_update_handlers.get(event.kind)
            if handler is None:
                logger.debug(
                    "Workflow %s received history event kind %s",
                    self.run_info.id,
                    event.kind,
                )
                return

            handler(event)

        except Exception as e:
            logger.exception("Error handling run event")
            if not self.done():
                self.set_exception(e)

    def _on_non_terminal_event(self, event: HistoryEvent) -> None:
        logger.debug(
            "Run %s received non-terminal history event %s",
            self.run_info.id,
            event.kind,
        )

    def _on_run_completed(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunCompleted):
            logger.error("Run %s completed event payload mismatch: %s", self.run_info.id, type(payload))
            return
        self.set_result(payload.result)

    def _on_run_failed(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunFailed):
            logger.error("Run %s failed event payload mismatch: %s", self.run_info.id, type(payload))
            return
        logger.debug("Workflow failed with error: %s", payload)
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
            logger.error("Run %s timeout payload mismatch: %s", self.run_info.id, type(payload))
            return
        error_msg = f"Workflow timed out after {payload.duration_ms}s"
        self.set_exception(TimeoutError(error_msg))

    def _on_run_cancelled(self, event: HistoryEvent) -> None:
        if self.done():
            return
        payload = event.msg
        if not isinstance(payload, RunCancelled):
            logger.error("Run %s cancel payload mismatch: %s", self.run_info.id, type(payload))
            return
        self.set_exception(asyncio.CancelledError("Workflow cancelled"))


async def create_workflow_future(
    run_info: RunInfo,
    nc: NATSClient,
    payload: Any | None = None,
) -> WorkflowFuture:
    """Create and start a WorkflowFuture for the given WorkflowRun."""
    return WorkflowFuture(run_info, nc, payload)
