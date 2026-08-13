import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any, Protocol, TypeVar, overload

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.codec import Codec
from grctl.exec.drc_factory import DrcFactory
from grctl.exec.journal import Journal
from grctl.exec.operations import Now, Random, SendToParent, Sleep, StartChild, Uuid4
from grctl.exec.task import Task
from grctl.models import Directive, ErrorDetails, RunInfo
from grctl.models.directive import RetryPolicy
from grctl.workflow.handle import WorkflowAPI, WorkflowHandle, WorkflowHandleFactory

StepHandler = Callable[..., Awaitable[Directive]]
T = TypeVar("T")


class Next(Protocol):
    """Build the workflow transition returned by a step handler."""

    def step(self, step_fn: StepHandler) -> Directive: ...

    def wait(self, timeout: timedelta | None = None, on_timeout: StepHandler | None = None) -> Directive: ...

    def complete(self, result: Any = None) -> Directive: ...

    def fail(self, error: ErrorDetails) -> Directive: ...


class Store(Protocol):
    """Read and stage updates to durable workflow state."""

    @overload
    async def get(self, key: str) -> Any: ...

    @overload
    async def get(self, key: str, ty: type[T]) -> T: ...

    def set(self, key: str, value: Any) -> None: ...


class Context:
    """Call-side entrypoint handed to user step functions."""

    def __init__(  # noqa: PLR0913
        self,
        journal: Journal,
        run_info: RunInfo,
        worker_id: str,
        step_name: str,
        drc_factory: DrcFactory,
        store: Store,
        workflow_api: WorkflowAPI,
        handle_factory: WorkflowHandleFactory,
        childs: ChildTracker,
        codec: Codec,
        parent_run: RunInfo | None = None,
    ) -> None:
        self._journal = journal
        self._run_info = run_info
        self._worker_id = worker_id
        self._step_name = step_name
        self._drc_factory = drc_factory
        self._store = store
        self._workflow_api = workflow_api
        self._handle_factory = handle_factory
        self._childs = childs
        self._codec = codec
        self._parent_run = parent_run

    @property
    def run_info(self) -> RunInfo:
        """The run this step belongs to — its ids, its type, and its parent if it has one."""
        return self._run_info

    @property
    def next(self) -> Next:
        """Build the workflow transition returned by this step."""
        return self._drc_factory

    @property
    def store(self) -> Store:
        """Read and update durable state for this workflow run."""
        return self._store

    async def run(self, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """Journal a plain async call as a task, without a retry policy."""
        return await self.run_task(fn, args, kwargs)

    async def run_task(
        self,
        fn: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        retry_policy: RetryPolicy | None = None,
    ) -> Any:
        """Journal a task call. Args are passed explicitly so no task parameter name is reserved."""
        task = Task(fn, args, kwargs, self._codec, self._step_name, retry_policy)
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
        operation = SendToParent(
            self._parent_run,
            self._worker_id,
            self._workflow_api,
            self._codec,
            event_name,
            payload,
        )
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
            self._codec,
            self._handle_factory,
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
        return await handle.result(timeout=timeout)

    def _callback_step_name(self, on_completed_step: StepHandler | None) -> str | None:
        if on_completed_step is None:
            return None
        return self._drc_factory.step_name(on_completed_step)
