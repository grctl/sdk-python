import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from logging import Logger
from random import random as _random
from typing import Any

from ulid import ULID

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.journal import OperationProgress, Outcome, identify
from grctl.models import (
    ChildWorkflowStarted,
    HistoryKind,
    ParentEventSent,
    RandomRecorded,
    RunInfo,
    SleepRecorded,
    TimestampRecorded,
    UuidRecorded,
)
from grctl.models.history import HistoryEvents
from grctl.workflow.future import HistoryListenerFactory
from grctl.workflow.handle import WorkflowAPI, WorkflowHandle


class Now:
    """Records the current time so replay reproduces the same value."""

    @property
    def name(self) -> str:
        return "now"

    def operation_id(self, seq: int) -> str:
        return identify(self.name, seq)

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return frozenset({HistoryKind.timestamp_recorded})

    async def perform(self, _progress: OperationProgress) -> Outcome:
        return HistoryKind.timestamp_recorded, TimestampRecorded(value=datetime.now(UTC))

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> datetime:  # noqa: ARG002
        if not isinstance(payload, TimestampRecorded):
            raise TypeError(f"Expected TimestampRecorded payload, got {type(payload)}")
        return payload.value


class Random:
    """Records a random float so replay reproduces the same value."""

    @property
    def name(self) -> str:
        return "random"

    def operation_id(self, seq: int) -> str:
        return identify(self.name, seq)

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return frozenset({HistoryKind.random_recorded})

    async def perform(self, _progress: OperationProgress) -> Outcome:
        return HistoryKind.random_recorded, RandomRecorded(value=_random())  # noqa: S311

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> float:  # noqa: ARG002
        if not isinstance(payload, RandomRecorded):
            raise TypeError(f"Expected RandomRecorded payload, got {type(payload)}")
        return payload.value


class Uuid4:
    """Records a UUID4 so replay reproduces the same value."""

    @property
    def name(self) -> str:
        return "uuid4"

    def operation_id(self, seq: int) -> str:
        return identify(self.name, seq)

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return frozenset({HistoryKind.uuid_recorded})

    async def perform(self, _progress: OperationProgress) -> Outcome:
        return HistoryKind.uuid_recorded, UuidRecorded(value=str(uuid.uuid4()))

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> uuid.UUID:  # noqa: ARG002
        if not isinstance(payload, UuidRecorded):
            raise TypeError(f"Expected UuidRecorded payload, got {type(payload)}")
        return uuid.UUID(payload.value)


class Sleep:
    """Sleeps for a duration on live execution; replay skips the actual wait."""

    def __init__(self, duration: timedelta) -> None:
        self._duration = duration
        self._duration_ms = int(duration.total_seconds() * 1000)

    @property
    def name(self) -> str:
        return "sleep"

    def operation_id(self, seq: int) -> str:
        return identify(self.name, seq, {"duration_ms": self._duration_ms})

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return frozenset({HistoryKind.sleep_recorded})

    async def perform(self, _progress: OperationProgress) -> Outcome:
        await asyncio.sleep(self._duration.total_seconds())
        return HistoryKind.sleep_recorded, SleepRecorded(duration_ms=self._duration_ms)

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> None:  # noqa: ARG002
        if not isinstance(payload, SleepRecorded):
            raise TypeError(f"Expected SleepRecorded payload, got {type(payload)}")


class StartChild:
    """Starts a child workflow, recording its run id so replay reconstructs the same handle.

    perform() only runs on live execution — it builds the handle and publishes the start
    command. On replay, materialize() rebuilds an equivalent handle from the recorded run id
    without re-publishing anything.
    """

    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        worker_id: str,
        workflow_api: WorkflowAPI,
        listener_factory: HistoryListenerFactory,
        logger: Logger,
        childs: ChildTracker,
        workflow_type: str,
        workflow_id: str,
        workflow_input: dict[str, Any] | None = None,
        workflow_timeout: timedelta | None = None,
        callback_step_name: str | None = None,
    ) -> None:
        self._run_info = run_info
        self._worker_id = worker_id
        self._workflow_api = workflow_api
        self._listener_factory = listener_factory
        self._logger = logger
        self._childs = childs
        self._workflow_type = workflow_type
        self._workflow_id = workflow_id
        self._workflow_input = workflow_input
        self._workflow_timeout = workflow_timeout
        self._callback_step_name = callback_step_name
        self._handle: WorkflowHandle | None = None

    @property
    def name(self) -> str:
        return "start_child"

    def operation_id(self, seq: int) -> str:
        return identify(
            self.name,
            seq,
            {
                "wf_type": self._workflow_type,
                "wf_id": self._workflow_id,
                "workflow_input": self._workflow_input,
                "workflow_timeout": int(self._workflow_timeout.total_seconds()) if self._workflow_timeout else None,
            },
        )

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return frozenset({HistoryKind.child_started})

    async def perform(self, _progress: OperationProgress) -> Outcome:
        run_id = str(ULID())
        self._handle = self._build_handle(run_id, self._workflow_input)
        await self._handle.start()
        return HistoryKind.child_started, ChildWorkflowStarted(
            run_id=run_id, wf_type=self._workflow_type, wf_id=self._workflow_id, input=self._workflow_input
        )

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> WorkflowHandle:  # noqa: ARG002
        if not isinstance(payload, ChildWorkflowStarted):
            raise TypeError(f"Expected ChildWorkflowStarted payload, got {type(payload)}")
        if self._handle is None:
            self._handle = self._build_handle(payload.run_id, payload.input)
        self._childs.add(self._handle)
        return self._handle

    def _build_handle(self, run_id: str, workflow_input: dict[str, Any] | None) -> WorkflowHandle:
        child_run_info = RunInfo(
            id=run_id,
            wf_type=self._workflow_type,
            wf_id=self._workflow_id,
            timeout=int(self._workflow_timeout.total_seconds()) if self._workflow_timeout else None,
            parent_wf_id=self._run_info.wf_id,
            parent_wf_type=self._run_info.wf_type,
            parent_run_id=self._run_info.id,
            parent_callback_step=self._callback_step_name,
            created_at=datetime.now(UTC),
        )
        return WorkflowHandle(
            run_info=child_run_info,
            payload=workflow_input,
            workflow_api=self._workflow_api,
            listener_factory=self._listener_factory,
            sender_id=self._worker_id,
            logger=self._logger,
        )


class SendToParent:
    """Emits an event to the parent workflow, recording it so replay doesn't republish it."""

    def __init__(
        self,
        parent_run: RunInfo | None,
        worker_id: str,
        workflow_api: WorkflowAPI,
        event_name: str,
        payload: Any | None = None,
    ) -> None:
        if parent_run is None:
            raise RuntimeError("No parent workflow to send event to.")
        self._parent_run = parent_run
        self._worker_id = worker_id
        self._workflow_api = workflow_api
        self._event_name = event_name
        self._payload = payload

    @property
    def name(self) -> str:
        return "send_to_parent"

    def operation_id(self, seq: int) -> str:
        return identify(self.name, seq, {"event_name": self._event_name, "payload": self._payload})

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]:
        return frozenset({HistoryKind.parent_event_sent})

    async def perform(self, _progress: OperationProgress) -> Outcome:
        await self._workflow_api.send_event(self._parent_run, self._event_name, self._payload, self._worker_id)
        return HistoryKind.parent_event_sent, ParentEventSent(
            event_name=self._event_name,
            payload=self._payload,
            parent_wf_type=self._parent_run.wf_type,
            parent_wf_id=self._parent_run.wf_id,
        )

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> None:  # noqa: ARG002
        if not isinstance(payload, ParentEventSent):
            raise TypeError(f"Expected ParentEventSent payload, got {type(payload)}")
