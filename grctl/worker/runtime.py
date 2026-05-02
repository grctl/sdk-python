import asyncio
import hashlib
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import msgspec

from grctl.models import Directive, HistoryEvent, HistoryKind, RunInfo
from grctl.models.history import HistoryEvents
from grctl.nats.connection import Connection
from grctl.nats.kv_store import KVStore
from grctl.worker.codec import CodecRegistry
from grctl.worker.store import Store
from grctl.workflow import Workflow

if TYPE_CHECKING:
    import logging

    from grctl.worker.context import Context

_step_run_time = ContextVar("step_run_time")


def _generate_operation_id(fn_name: str, args: dict[str, Any], seq: int) -> str:
    data = msgspec.msgpack.encode({"args": args, "seq": seq})
    digest = hashlib.sha256(data).hexdigest()[:16]
    return f"{fn_name}:{digest}"


class NonDeterminismError(Exception):
    """Raised when replay history doesn't match current execution order."""


# Only these kinds participate in replay history matching — everything else is observability-only
_REPLAY_KINDS = frozenset(
    {
        HistoryKind.task_completed,
        HistoryKind.task_failed,
        HistoryKind.task_cancelled,
        HistoryKind.event_received,
        HistoryKind.timestamp_recorded,
        HistoryKind.random_recorded,
        HistoryKind.uuid_recorded,
        HistoryKind.sleep_recorded,
        HistoryKind.child_started,
        HistoryKind.parent_event_sent,
    }
)


class StepRuntime:
    def __init__(  # noqa: PLR0913
        self,
        workflow: Workflow,
        worker_id: str,
        directive: Directive,
        connection: Connection,
        step_history: list[HistoryEvent] | None = None,
        workflow_logger: "logging.Logger | None" = None,
    ) -> None:
        self.run_info = directive.run_info
        self.workflow = workflow
        self.worker_id = worker_id
        self.directive = directive
        self.connection = connection
        self.publisher = connection.publisher
        self.codec = CodecRegistry()
        self.store = self._create_store()
        self.step_history = step_history
        self.workflow_logger = workflow_logger
        self.step_name: str
        self.parent_run = self._create_parent_run()
        self._seq: int = 0
        self._cursor: int = 0
        self._pending: dict[str, tuple[asyncio.Future[HistoryEvents], frozenset[HistoryKind]]] = {}

    def generate_operation_id(self, fn_name: str, args: dict[str, Any]) -> str:
        self._seq += 1
        return _generate_operation_id(fn_name, args, self._seq)

    @property
    def is_replaying(self) -> bool:
        return bool(self.step_history) and self._cursor < len(self.step_history)

    async def next(
        self, acceptable_kinds: HistoryKind | frozenset[HistoryKind], operation_id: str
    ) -> asyncio.Future[HistoryEvents] | None:

        # Check if we have a step history and the cursor is within bounds. If not, we are not replaying — return None.
        if not self.step_history or self._cursor >= len(self.step_history):
            return None

        kinds = acceptable_kinds if isinstance(acceptable_kinds, frozenset) else frozenset({acceptable_kinds})
        loop = asyncio.get_running_loop()
        future: asyncio.Future[HistoryEvents] = loop.create_future()
        self._pending[operation_id] = (future, kinds)

        self._resolve()
        await asyncio.sleep(0)
        self._resolve()

        if not future.done():
            if self._cursor >= len(self.step_history):
                self._pending.pop(operation_id, None)
                return None  # history exhausted — live execution
            raise NonDeterminismError(
                f"Unresolved operation {operation_id} ({acceptable_kinds}) after yield — "
                f"cursor at {self._cursor}, pending: {list(self._pending.keys())}"
            )

        return future

    def _resolve(self) -> None:
        if self.step_history is None:
            return
        while self._cursor < len(self.step_history):
            entry = self.step_history[self._cursor]
            # Skip observability-only events that don't participate in replay matching
            if entry.kind not in _REPLAY_KINDS:
                self._cursor += 1
                continue
            if entry.operation_id not in self._pending:
                break
            future, acceptable_kinds = self._pending.pop(entry.operation_id)
            if entry.kind not in acceptable_kinds:
                future.set_exception(
                    NonDeterminismError(
                        f"Expected one of {acceptable_kinds} but history has {entry.kind} "
                        f"at cursor {self._cursor} for {entry.operation_id}"
                    )
                )
            else:
                future.set_result(entry.msg)
            self._cursor += 1

    async def record(self, kind: HistoryKind, payload: HistoryEvents, operation_id: str) -> None:
        event = self._create_history_event(kind, payload, operation_id)
        await self.publisher.publish_history(run_info=self.run_info, event=event, enc_hook=self.codec.enc_hook)

    def get_step_context(self) -> "Context":
        from grctl.worker.context import Context  # noqa: PLC0415

        return Context(
            run_info=self.run_info,
            store=self.store,
            worker_id=self.worker_id,
            directive=self.directive,
            parent_run=self.parent_run,
            step_configs=self.workflow._step_handlers,  # noqa: SLF001
            workflow_logger=self.workflow_logger,
        )

    def _create_store(self) -> Store:
        kv_store = KVStore(self.connection.js, self.connection.manifest, self.run_info)
        return Store(loader=kv_store.load, codec=self.codec)

    def _create_parent_run(self) -> RunInfo | None:
        if self.run_info.parent_run_id and self.run_info.parent_wf_id:
            return RunInfo(
                id=self.run_info.parent_run_id,
                wf_id=self.run_info.parent_wf_id,
                wf_type=self.run_info.parent_wf_type or "",
            )
        return None

    def _create_history_event(self, kind: HistoryKind, payload: HistoryEvents, operation_id: str) -> HistoryEvent:
        """Wrap a step history payload with the shared event metadata."""
        return HistoryEvent(
            wf_id=self.run_info.wf_id,
            run_id=self.run_info.id,
            worker_id=self.worker_id,
            timestamp=datetime.now(UTC),
            kind=kind,
            msg=payload,
            operation_id=operation_id,
        )


def get_step_runtime() -> StepRuntime:
    return _step_run_time.get()


def set_step_runtime(runtime: StepRuntime) -> Token:
    return _step_run_time.set(runtime)
