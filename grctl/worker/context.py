import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from random import random as _random
from typing import Any, cast

from ulid import ULID

from grctl.models import (
    ChildWorkflowStarted,
    CmdKind,
    Command,
    Directive,
    EventCmd,
    HistoryKind,
    ParentEventSent,
    RandomRecorded,
    RunInfo,
    SleepRecorded,
    TimestampRecorded,
    UuidRecorded,
)
from grctl.worker.kv_store import KVStore
from grctl.worker.logger import ReplayFilter
from grctl.worker.next_directive_builder import NextDirectiveBuilder
from grctl.worker.runtime import StepRuntime, get_step_runtime
from grctl.workflow import WorkflowHandle
from grctl.workflow.workflow import HandlerConfig

StepHandler = Callable[..., Awaitable[Directive]]


class Context:
    """Context for a workflow run execution.

    Holds all dependencies and metadata needed to execute a workflow run.
    """

    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        kv_store: KVStore,
        worker_id: str,
        directive: Directive,
        runtime: StepRuntime,
        parent_run: RunInfo | None = None,
        step_configs: dict[str, HandlerConfig] | None = None,
    ) -> None:
        self.run = run_info
        self._kv_store = kv_store
        self._worker_id = worker_id
        self._next_builder = NextDirectiveBuilder(run_info, worker_id, kv_store, directive, step_configs)
        self._parent_run = parent_run
        # Child handles started during this step. They are single-step-scoped: cross-step
        # coordination uses events/callbacks, not in-memory futures, so any handle still
        # open when the step returns is abandoned and gets discarded.
        self._started_handles: list[WorkflowHandle] = []
        self.runtime = runtime

    @property
    def store(self) -> KVStore:
        return self._kv_store

    @property
    def next(self) -> NextDirectiveBuilder:
        return self._next_builder

    @property
    def logger(self) -> logging.Logger:
        logger = logging.getLogger(f"grctl.workflow.{self.run.wf_type}")
        if not any(isinstance(f, ReplayFilter) for f in logger.filters):

            def _is_replaying() -> bool:
                try:
                    return get_step_runtime().is_replaying
                except LookupError:
                    return False

            logger.addFilter(ReplayFilter(_is_replaying))
        return logger

    async def send_to_parent(self, event_name: str, payload: Any | None = None) -> None:
        """Emit an event to the parent workflow, if any."""
        if self._parent_run is None:
            raise RuntimeError("No parent workflow to send event to.")

        operation_id = self.runtime.generate_operation_id(
            "send_to_parent", {"event_name": event_name, "payload": payload}
        )
        future = await self.runtime.next(HistoryKind.parent_event_sent, operation_id)
        if future is not None:
            future.result()  # surfaces NonDeterminismError on kind mismatch
            return

        await self.runtime.publisher.publish_cmd(
            self._parent_run,
            Command(
                id=str(ULID()),
                kind=CmdKind.run_event,
                timestamp=datetime.now(UTC),
                sender_id=self._worker_id,
                msg=EventCmd(
                    wf_id=self._parent_run.wf_id,
                    event_name=event_name,
                    payload=payload,
                ),
            ),
        )
        await self.runtime.record(
            HistoryKind.parent_event_sent,
            ParentEventSent(
                event_name=event_name,
                payload=payload,
                parent_wf_type=self._parent_run.wf_type,
                parent_wf_id=self._parent_run.wf_id,
            ),
            operation_id,
        )

    async def start_child(
        self,
        workflow_type: str,
        workflow_id: str,
        workflow_input: dict[str, Any] | None = None,
        workflow_timeout: timedelta | None = None,
        on_completed_step: StepHandler | None = None,
    ) -> WorkflowHandle:
        """Start a child workflow and return its handle.

        When on_completed_step is given, the server triggers that parent step once the
        child reaches any terminal state, passing a ChildOutcome describing the result
        (on success) or the error (on failure/cancellation). The parent typically parks
        with ctx.next.wait() so the callback can wake it.
        """
        callback_step_name = self._callback_step_name(on_completed_step)

        operation_id = self.runtime.generate_operation_id(
            "start",
            {
                "wf_type": workflow_type,
                "wf_id": workflow_id,
                "workflow_input": workflow_input,
                "workflow_timeout": int(workflow_timeout.total_seconds()) if workflow_timeout else None,
            },
        )
        future = await self.runtime.next(HistoryKind.child_started, operation_id)

        run_id = cast("ChildWorkflowStarted", future.result()).run_id if future is not None else str(ULID())

        run_info = RunInfo(
            id=run_id,
            wf_type=workflow_type,
            wf_id=workflow_id,
            timeout=int(workflow_timeout.total_seconds()) if workflow_timeout else None,
            parent_wf_id=self.run.wf_id,
            parent_wf_type=self.run.wf_type,
            parent_run_id=self.run.id,
            parent_callback_step=callback_step_name,
            created_at=datetime.now(UTC),
        )
        handle = WorkflowHandle(
            run_info=run_info,
            payload=workflow_input,
            connection=self.runtime.connection,
            sender_id=self._worker_id,
        )
        self._started_handles.append(handle)

        if future is None:
            await handle.start()
            await self.runtime.record(
                HistoryKind.child_started,
                ChildWorkflowStarted(run_id=run_id, wf_type=workflow_type, wf_id=workflow_id, input=workflow_input),
                operation_id,
            )
        return handle

    async def run_child(
        self,
        workflow_type: str,
        workflow_id: str,
        workflow_input: dict[str, Any] | None = None,
        workflow_timeout: timedelta | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> Any:
        """Start a child workflow and block until it completes, returning its result.

        Raises WorkflowError on failure, asyncio.CancelledError on cancellation/termination,
        and TimeoutError if the client-side timeout elapses.
        timeout: client-side wait in seconds, independent of the server-side workflow_timeout.
        """
        handle = await self.start_child(workflow_type, workflow_id, workflow_input, workflow_timeout)
        if not handle.future.is_started:
            await handle.future.start()
        return await handle.result(timeout=timeout)

    async def _discard_started_handles(self) -> None:
        """Silently release child handles started during this step but not awaited.

        Called by the runner once the step returns its directive, on both the success
        and failure paths. Stops each handle's history subscription and swallows any
        pending child outcome so an unawaited future does not warn or leak a subscription.
        """
        for handle in self._started_handles:
            await handle.future.discard()
        self._started_handles.clear()

    @staticmethod
    def _callback_step_name(on_completed_step: StepHandler | None) -> str | None:
        if on_completed_step is None:
            return None
        step_name = getattr(on_completed_step, "__name__", None)
        if not step_name:
            raise ValueError("on_completed_step must be a named handler function.")
        return step_name

    async def now(self) -> datetime:
        operation_id = self.runtime.generate_operation_id("now", {})
        future = await self.runtime.next(HistoryKind.timestamp_recorded, operation_id)
        if future is not None:
            return cast("TimestampRecorded", future.result()).value
        value = datetime.now(UTC)
        await self.runtime.record(HistoryKind.timestamp_recorded, TimestampRecorded(value=value), operation_id)
        return value

    async def random(self) -> float:
        operation_id = self.runtime.generate_operation_id("random", {})
        future = await self.runtime.next(HistoryKind.random_recorded, operation_id)
        if future is not None:
            return cast("RandomRecorded", future.result()).value
        value = _random()  # noqa: S311
        await self.runtime.record(HistoryKind.random_recorded, RandomRecorded(value=value), operation_id)
        return value

    async def uuid4(self) -> uuid.UUID:
        operation_id = self.runtime.generate_operation_id("uuid4", {})
        future = await self.runtime.next(HistoryKind.uuid_recorded, operation_id)
        if future is not None:
            return uuid.UUID(cast("UuidRecorded", future.result()).value)
        value = uuid.uuid4()
        await self.runtime.record(HistoryKind.uuid_recorded, UuidRecorded(value=str(value)), operation_id)
        return value

    async def sleep(self, duration: timedelta) -> None:
        duration_ms = int(duration.total_seconds() * 1000)
        operation_id = self.runtime.generate_operation_id("sleep", {"duration_ms": duration_ms})
        future = await self.runtime.next(HistoryKind.sleep_recorded, operation_id)
        if future is not None:
            return
        await asyncio.sleep(duration.total_seconds())
        await self.runtime.record(HistoryKind.sleep_recorded, SleepRecorded(duration_ms=duration_ms), operation_id)
