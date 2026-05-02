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
    Complete,
    Directive,
    DirectiveKind,
    ErrorDetails,
    EventCmd,
    Fail,
    HistoryKind,
    ParentEventSent,
    RandomRecorded,
    RunInfo,
    SleepRecorded,
    Step,
    StepResult,
    TimestampRecorded,
    UuidRecorded,
    WaitEvent,
)
from grctl.worker.logger import ReplayAwareLogger
from grctl.worker.runtime import get_step_runtime
from grctl.worker.store import Store
from grctl.workflow import WorkflowHandle
from grctl.workflow.workflow import HandlerConfig

StepHandler = Callable[..., Awaitable[Directive]]


class NextBuilder:
    """Builder for creating step transition directives.

    Allows both ctx.next.step(step_func) and ctx.next.complete(result) syntax.
    """

    def __init__(
        self,
        run: RunInfo,
        worker_id: str,
        store: Store,
        current_directive: Directive,
        step_configs: dict[str, HandlerConfig] | None = None,
    ) -> None:
        self._run = run
        self._worker_id = worker_id
        self._store = store
        self._current_directive = current_directive
        self._step_configs = step_configs or {}

    def step(self, step_fn: StepHandler) -> Directive:
        step_name = getattr(step_fn, "__name__", None)
        if step_name is None:
            raise ValueError("Step function must have a __name__ attribute.")

        config = self._step_configs.get(step_name)
        timeout_ms = int(config.timeout.total_seconds() * 1000) if config and config.timeout else None

        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.step,
            next_msg=Step(
                step_name=step_name,
                timeout_ms=timeout_ms,
            ),
        )

        return Directive(
            id=str(ULID()), kind=DirectiveKind.step_result, run_info=self._run, timestamp=datetime.now(UTC), msg=res
        )

    def wait_for_event(self, timeout: timedelta | None = None, timeout_step_name: str | None = None) -> Directive:
        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.wait_event,
            next_msg=WaitEvent(
                timeout_ms=int(timeout.total_seconds() * 1000) if timeout else 0,
                timeout_step_name=timeout_step_name,
            ),
        )

        return Directive(
            id=str(ULID()), kind=DirectiveKind.step_result, run_info=self._run, timestamp=datetime.now(UTC), msg=res
        )

    def complete(self, result: Any) -> Directive:
        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.complete,
            next_msg=Complete(
                result=result,
            ),
        )

        return Directive(
            id=str(ULID()), kind=DirectiveKind.step_result, run_info=self._run, timestamp=datetime.now(UTC), msg=res
        )

    def fail(self, error: ErrorDetails) -> Directive:
        res = StepResult(
            processed_msg_kind=self._current_directive.kind,
            processed_msg=self._current_directive.msg,
            worker_id=self._worker_id,
            kv_updates=self._store.get_pending_updates() or {},
            next_msg_kind=DirectiveKind.fail,
            next_msg=Fail(
                error=error,
            ),
        )

        return Directive(
            id=str(ULID()),
            kind=DirectiveKind.step_result,
            run_info=self._run,
            timestamp=datetime.now(UTC),
            msg=res,
        )


class Context:
    """Context for a workflow run execution.

    Holds all dependencies and metadata needed to execute a workflow run.
    """

    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        store: Store,
        worker_id: str,
        directive: Directive,
        parent_run: RunInfo | None = None,
        step_configs: dict[str, HandlerConfig] | None = None,
        workflow_logger: logging.Logger | None = None,
    ) -> None:
        self.run = run_info
        self._store = store
        self._worker_id = worker_id
        self._next_builder = NextBuilder(run_info, worker_id, store, directive, step_configs)
        self._parent_run = parent_run
        self._workflow_logger = workflow_logger

    @property
    def store(self) -> Store:
        return self._store

    @property
    def next(self) -> NextBuilder:
        return self._next_builder

    @property
    def logger(self) -> ReplayAwareLogger:
        return ReplayAwareLogger(self.run.wf_type, self._workflow_logger)

    async def send_to_parent(self, event_name: str, payload: Any | None = None) -> None:
        """Emit an event to the parent workflow, if any."""
        if self._parent_run is None:
            raise RuntimeError("No parent workflow to send event to.")

        runtime = get_step_runtime()
        operation_id = runtime.generate_operation_id("send_to_parent", {"event_name": event_name, "payload": payload})
        future = await runtime.next(HistoryKind.parent_event_sent, operation_id)
        if future is not None:
            future.result()  # surfaces NonDeterminismError on kind mismatch
            return

        await runtime.publisher.publish_cmd(
            self._parent_run,
            Command(
                id=str(ULID()),
                kind=CmdKind.run_event,
                timestamp=datetime.now(UTC),
                msg=EventCmd(
                    wf_id=self._parent_run.wf_id,
                    event_name=event_name,
                    payload=payload,
                ),
            ),
        )
        await runtime.record(
            HistoryKind.parent_event_sent,
            ParentEventSent(
                event_name=event_name,
                payload=payload,
                parent_wf_type=self._parent_run.wf_type,
                parent_wf_id=self._parent_run.wf_id,
            ),
            operation_id,
        )

    async def start(
        self,
        workflow_type: str,
        workflow_id: str,
        workflow_input: dict[str, Any] | None = None,
        workflow_timeout: timedelta | None = None,
    ) -> WorkflowHandle:
        """Start a child workflow and return its handle."""
        runtime = get_step_runtime()
        operation_id = runtime.generate_operation_id(
            "start",
            {
                "wf_type": workflow_type,
                "wf_id": workflow_id,
                "workflow_input": workflow_input,
                "workflow_timeout": int(workflow_timeout.total_seconds()) if workflow_timeout else None,
            },
        )
        future = await runtime.next(HistoryKind.child_started, operation_id)

        run_id = cast("ChildWorkflowStarted", future.result()).run_id if future is not None else str(ULID())

        run_info = RunInfo(
            id=run_id,
            wf_type=workflow_type,
            wf_id=workflow_id,
            timeout=int(workflow_timeout.total_seconds()) if workflow_timeout else None,
            parent_wf_id=self.run.wf_id,
            parent_wf_type=self.run.wf_type,
            parent_run_id=self.run.id,
            created_at=datetime.now(UTC),
        )
        handle = WorkflowHandle(
            run_info=run_info,
            payload=workflow_input,
            connection=runtime.connection,
        )

        if future is None:
            await handle.start()
            await runtime.record(
                HistoryKind.child_started,
                ChildWorkflowStarted(run_id=run_id, wf_type=workflow_type, wf_id=workflow_id, input=workflow_input),
                operation_id,
            )
        return handle

    async def now(self) -> datetime:
        runtime = get_step_runtime()
        operation_id = runtime.generate_operation_id("now", {})
        future = await runtime.next(HistoryKind.timestamp_recorded, operation_id)
        if future is not None:
            return cast("TimestampRecorded", future.result()).value
        value = datetime.now(UTC)
        await runtime.record(HistoryKind.timestamp_recorded, TimestampRecorded(value=value), operation_id)
        return value

    async def random(self) -> float:
        runtime = get_step_runtime()
        operation_id = runtime.generate_operation_id("random", {})
        future = await runtime.next(HistoryKind.random_recorded, operation_id)
        if future is not None:
            return cast("RandomRecorded", future.result()).value
        value = _random()  # noqa: S311
        await runtime.record(HistoryKind.random_recorded, RandomRecorded(value=value), operation_id)
        return value

    async def uuid4(self) -> uuid.UUID:
        runtime = get_step_runtime()
        operation_id = runtime.generate_operation_id("uuid4", {})
        future = await runtime.next(HistoryKind.uuid_recorded, operation_id)
        if future is not None:
            return uuid.UUID(cast("UuidRecorded", future.result()).value)
        value = uuid.uuid4()
        await runtime.record(HistoryKind.uuid_recorded, UuidRecorded(value=str(value)), operation_id)
        return value

    async def sleep(self, duration: timedelta) -> None:
        runtime = get_step_runtime()
        duration_ms = int(duration.total_seconds() * 1000)
        operation_id = runtime.generate_operation_id("sleep", {"duration_ms": duration_ms})
        future = await runtime.next(HistoryKind.sleep_recorded, operation_id)
        if future is not None:
            return
        await asyncio.sleep(duration.total_seconds())
        await runtime.record(HistoryKind.sleep_recorded, SleepRecorded(duration_ms=duration_ms), operation_id)
