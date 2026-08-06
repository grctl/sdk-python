import asyncio
from datetime import UTC, datetime
from typing import Any, NamedTuple, Protocol

from grctl.exec.step_history import HistoryCreateInput
from grctl.models import HistoryEvent, HistoryKind
from grctl.models.history import HistoryEvents
from grctl.serde import fingerprint

Outcome = tuple[HistoryKind, HistoryEvents]


def identify(name: str, seq: int, args: dict[str, Any] | None = None) -> str:
    """Build an operation id from a name, a call position, and what makes the call distinct.

    Operations compose their own ids through this so the format stays uniform, and so
    the readable part survives: the id is copied into task history and shown to whoever
    is reading a run back, who needs to see which call it was and not only that two
    digests differ.
    """
    if not args:
        return f"{name}:{seq}"
    return f"{name}:{seq}:{fingerprint(args)}"


class NonDeterminismError(Exception):
    """Raised when replay history doesn't match current execution order."""


class PendingOperation(NamedTuple):
    """An operation awaiting its outcome from a not-yet-reached point in step_history."""

    future: asyncio.Future[Outcome]
    acceptable_kinds: frozenset[HistoryKind]


class StepHistoryAppender(Protocol):
    """Durable sink this journal records into — defined here, close to its only caller."""

    async def append(self, entry: HistoryCreateInput) -> None: ...


class OperationProgress:
    """What an operation may report while it is still running, and what it reported before.

    Most operations resolve in one shot and never touch this. A task with a retry
    policy does not: it makes several attempts, and the record of the attempts that
    failed has to outlive the worker making them. Attempts already recorded are how a
    task that gets re-delivered after its worker died knows not to start its attempt
    budget over.

    Entries recorded here are observability-only — they are not outcomes, take no part
    in replay matching, and never resolve the operation.
    """

    def __init__(self, operation_id: str, step_history: list[HistoryEvent], journal: "Journal") -> None:
        self.operation_id = operation_id
        self._step_history = step_history
        self._journal = journal

    def count(self, kind: HistoryKind) -> int:
        """Entries of `kind` earlier attempts of this same operation already recorded."""
        return sum(1 for e in self._step_history if e.kind == kind and e.operation_id == self.operation_id)

    async def record(self, kind: HistoryKind, payload: HistoryEvents) -> None:
        await self._journal.record(kind, payload, self.operation_id)


class Operation(Protocol):
    """A single durable unit of work: a task call, a sleep, ctx.now(), ctx.uuid4(), etc.

    All operation kinds implement this same shape — the journal doesn't know or
    care which one it's running.
    """

    @property
    def name(self) -> str: ...

    def operation_id(self, seq: int) -> str:
        """Identify this call for replay matching, at call position `seq` within the step.

        Two executions of a step that reached this point having done the same things must
        produce the same id, and any difference that would change the outcome must produce
        a different one — otherwise replay hands back a result computed from an input this
        run never produced.

        `seq` covers call position. Everything else is the operation's own business: a
        task's arguments, a child's workflow id, a sleep's duration. An input left out of
        the id is a divergence replay will not catch.
        """
        ...

    @property
    def acceptable_kinds(self) -> frozenset[HistoryKind]: ...

    async def perform(self, progress: OperationProgress, /) -> Outcome:
        """Run the underlying work. Never raises — failure becomes data.

        `progress` is this operation's own slice of the journal. Operations that resolve
        in one shot ignore it.
        """
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

    @property
    def is_replaying(self) -> bool:
        return self._cursor < len(self.step_history)

    async def run(self, operation: Operation) -> Any:
        """Perform an operation, or replay the outcome history already holds for it.

        The journal owns call position and nothing else about identity: it hands the
        operation its sequence number and the operation says what it is.
        """
        self._seq += 1
        operation_id = operation.operation_id(self._seq)

        future = await self.next(operation.acceptable_kinds, operation_id)
        if future is not None:
            kind, payload = await future
        else:
            progress = OperationProgress(operation_id, self.step_history, self)
            kind, payload = await operation.perform(progress)
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
            raise NonDeterminismError(self._divergence(operation_id))

        return future

    def _divergence(self, operation_id: str) -> str:
        """Describe how the code diverged from history, for whoever has to read it back.

        Both ids are shown because the difference between them is the diagnosis: a
        different name means the code calls something else here, a different position
        means calls were reordered or one was added, a different fingerprint means the
        same call ran on different inputs.
        """
        recorded = self.step_history[self._cursor]
        return (
            f"Step replay diverged at history position {self._cursor}.\n"
            f"  history recorded: {recorded.operation_id} ({recorded.kind})\n"
            f"  code called:      {operation_id}\n"
            "The recorded call and this one differ in name, call order, or argument values."
        )

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
