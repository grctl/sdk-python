import asyncio
import hashlib
from datetime import UTC, datetime
from typing import Any, NamedTuple, Protocol

import msgspec

from grctl.exec.step_history import HistoryCreateInput
from grctl.models import HistoryEvent, HistoryKind
from grctl.models.history import HistoryEvents

Outcome = tuple[HistoryKind, HistoryEvents]


class NonDeterminismError(Exception):
    """Raised when replay history doesn't match current execution order."""


class PendingOperation(NamedTuple):
    """An operation awaiting its outcome from a not-yet-reached point in step_history."""

    future: asyncio.Future[Outcome]
    acceptable_kinds: frozenset[HistoryKind]


class StepHistoryAppender(Protocol):
    """Durable sink this journal records into — defined here, close to its only caller."""

    async def append(self, entry: HistoryCreateInput) -> None: ...


class Operation(Protocol):
    """A single durable unit of work: a task call, a sleep, ctx.now(), ctx.uuid4(), etc.

    All operation kinds implement this same shape — the journal doesn't know or
    care which one it's running.
    """

    @property
    def name(self) -> str: ...

    @property
    def args(self) -> dict[str, Any]: ...

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]: ...

    async def perform(self) -> Outcome:
        """Run the underlying work. Never raises — failure becomes data."""
        ...

    def materialize(self, kind: HistoryKind, payload: HistoryEvents) -> Any:
        """Turn a (kind, payload) — recorded or replayed — into a value, or raise it."""
        ...


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


class Journal:
    def __init__(
        self,
        step_history: list[HistoryEvent],
        appender: StepHistoryAppender,
    ) -> None:
        self.step_history = step_history
        self._appender = appender
        self._seq: int = 0
        self._cursor: int = 0
        self._pending: dict[str, PendingOperation] = {}

    def generate_operation_id(self, fn_name: str, args: dict[str, Any]) -> str:
        self._seq += 1
        data = msgspec.msgpack.encode({"args": args, "seq": self._seq})
        digest = hashlib.sha256(data).hexdigest()[:16]
        return f"{fn_name}:{digest}"

    @property
    def is_replaying(self) -> bool:
        return self._cursor < len(self.step_history)

    async def run(self, operation: Operation) -> Any:
        operation_id = self.generate_operation_id(operation.name, operation.args)

        future = await self.next(operation.acceptable_kinds, operation_id)
        if future is not None:
            kind, payload = await future
        else:
            kind, payload = await operation.perform()
            await self.record(kind, payload, operation_id)

        return operation.materialize(kind, payload)

    async def next(self, acceptable_kinds: frozenset[HistoryKind], operation_id: str) -> asyncio.Future[Outcome] | None:

        # If the cursor is past the end of history, we are not replaying — return None.
        if self._cursor >= len(self.step_history):
            return None

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Outcome] = loop.create_future()
        self._pending[operation_id] = PendingOperation(future, acceptable_kinds)

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
        while self._cursor < len(self.step_history):
            entry = self.step_history[self._cursor]
            # Skip observability-only events that don't participate in replay matching
            if entry.kind not in _REPLAY_KINDS:
                self._cursor += 1
                continue
            if entry.operation_id not in self._pending:
                break
            pending = self._pending.pop(entry.operation_id)
            if entry.kind not in pending.acceptable_kinds:
                pending.future.set_exception(
                    NonDeterminismError(
                        f"Expected one of {pending.acceptable_kinds} but history has {entry.kind} "
                        f"at cursor {self._cursor} for {entry.operation_id}"
                    )
                )
            else:
                pending.future.set_result((entry.kind, entry.msg))
            self._cursor += 1

    async def record(self, kind: HistoryKind, payload: HistoryEvents, operation_id: str) -> None:
        entry = HistoryCreateInput(kind=kind, payload=payload, operation_id=operation_id, timestamp=datetime.now(UTC))
        await self._appender.append(entry)
