import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.journal import Journal
from grctl.exec.operations import Now, Random, SendToParent, Sleep, StartChild, Uuid4
from grctl.exec.task import Task
from grctl.models import Directive, RunInfo
from grctl.nats.connection import Connection
from grctl.workflow import WorkflowHandle

StepHandler = Callable[..., Awaitable[Directive]]


class Context:
    """Call-side entrypoint handed to user step functions."""

    def __init__(  # noqa: PLR0913
        self,
        journal: Journal,
        run_info: RunInfo,
        worker_id: str,
        connection: Connection,
        childs: ChildTracker,
        parent_run: RunInfo | None = None,
    ) -> None:
        self._journal = journal
        self._run_info = run_info
        self._worker_id = worker_id
        self._connection = connection
        self._childs = childs
        self._parent_run = parent_run

    async def run(self, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        task = Task(fn, args, kwargs)
        return await self._journal.run(task)

    async def now(self) -> datetime:
        return await self._journal.run(Now())

    async def random(self) -> float:
        return await self._journal.run(Random())

    async def uuid4(self) -> uuid.UUID:
        return await self._journal.run(Uuid4())

    async def sleep(self, duration: timedelta) -> None:
        await self._journal.run(Sleep(duration))

    async def send_to_parent(self, event_name: str, payload: Any | None = None) -> None:
        """Emit an event to the parent workflow, if any."""
        operation = SendToParent(self._parent_run, self._worker_id, self._connection, event_name, payload)
        await self._journal.run(operation)

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
        operation = StartChild(
            self._run_info,
            self._worker_id,
            self._connection,
            self._childs,
            workflow_type,
            workflow_id,
            workflow_input,
            workflow_timeout,
            self._callback_step_name(on_completed_step),
        )
        return await self._journal.run(operation)

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

    @staticmethod
    def _callback_step_name(on_completed_step: StepHandler | None) -> str | None:
        if on_completed_step is None:
            return None
        step_name = getattr(on_completed_step, "__name__", None)
        if not step_name:
            raise ValueError("on_completed_step must be a named handler function.")
        return step_name
