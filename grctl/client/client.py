"""Workflow Engine Client.

Provides a simple interface for interacting with workflows.
"""

import logging
import secrets
import socket
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, TypeVar, overload

from ulid import ULID

from grctl.exec.codec import Codec
from grctl.models import HistoryEvent, RunInfo
from grctl.workflow.future import HistoryListenerFactory
from grctl.workflow.handle import WorkflowAPI, WorkflowHandle, WorkflowHandleFactory

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class RunHistoryReader(Protocol):
    """Durable store a Client reads a completed or in-flight run's events from."""

    async def get_run_history(self, wf_id: str, run_id: str) -> list[HistoryEvent]: ...


class ClientWorkflowAPI(WorkflowAPI, Protocol):
    """The run-scoped server calls a Client makes, on top of what a handle needs.

    describe_run is the client's alone: a handle is always constructed around a
    run it already knows about.
    """

    async def describe_run(self, wf_id: str, sender_id: str) -> RunInfo: ...


class Connection(Protocol):
    """What a Client needs from a connection.

    A structural protocol rather than an import from nats/: client/ stays free
    of any transport dependency, and any backend (NATS, an in-memory fake for
    tests, ...) can satisfy this without inheriting from it.
    """

    @property
    def workflow_api(self) -> ClientWorkflowAPI: ...

    @property
    def listener_factory(self) -> HistoryListenerFactory: ...

    @property
    def history_reader(self) -> RunHistoryReader: ...

    @property
    def codec(self) -> Codec: ...


class Client:
    """Client for interacting with the Workflow Engine."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self.id = f"c_{secrets.token_hex(4)}@{socket.gethostname()}"
        self._handle_factory = WorkflowHandleFactory(
            workflow_api=connection.workflow_api,
            listener_factory=connection.listener_factory,
            sender_id=self.id,
            logger=logger,
            decoder=connection.codec,
        )

    async def describe(self, wf_id: str) -> RunInfo:
        """Describe the latest run for a workflow ID."""
        return await self._connection.workflow_api.describe_run(wf_id, self.id)

    @overload
    async def run_workflow(
        self,
        type: str,
        id: str,
        input: Any | None = ...,
        timeout: timedelta | None = ...,  # noqa: ASYNC109
        return_type: type[_T] = ...,
    ) -> _T: ...

    @overload
    async def run_workflow(
        self,
        type: str,
        id: str,
        input: Any | None = ...,
        timeout: timedelta | None = ...,  # noqa: ASYNC109
        return_type: None = ...,
    ) -> Any: ...

    async def run_workflow(
        self,
        type: str,  # noqa: A002
        id: str,  # noqa: A002
        input: Any | None = None,  # noqa: A002
        timeout: timedelta | None = None,  # noqa: ASYNC109
        return_type: type[_T] | None = None,
    ) -> _T | Any:
        """Run a workflow and wait for its result."""
        wf_handle = await self.start_workflow(
            type=type,
            id=id,
            input=input,
            timeout=timeout,
            return_type=return_type,
        )
        wait_timeout = timeout.total_seconds() if timeout else None
        return await wf_handle.result(timeout=wait_timeout)

    async def get_workflow_handle(self, wfid: str) -> WorkflowHandle:
        """Get a handle for an already-running workflow."""
        run_info = await self.describe(wfid)

        handle = self._handle_factory.create(
            run_info=run_info,
            payload=None,
        )
        await handle.attach()
        return handle

    async def get_history(self, wf_id: str, run_id: str | None = None) -> list[HistoryEvent]:
        """Return the ordered history events for a workflow run."""
        resolved_run_id = run_id
        if resolved_run_id is None:
            resolved_run_id = (await self.describe(wf_id)).id

        return await self._connection.history_reader.get_run_history(wf_id=wf_id, run_id=resolved_run_id)

    async def start_workflow(
        self,
        type: str,  # noqa: A002
        id: str,  # noqa: A002
        input: Any | None = None,  # noqa: A002
        timeout: timedelta | None = None,  # noqa: ASYNC109
        return_type: type | None = None,
    ) -> WorkflowHandle:
        """Start a workflow and return a handle to track and interact with it."""
        workflow_run_id = str(ULID())

        run_info = RunInfo(
            id=workflow_run_id,
            wf_type=type,
            wf_id=id,
            timeout=int(timeout.total_seconds()) if timeout else None,
            created_at=datetime.now(UTC),
        )

        handle = self._handle_factory.create(
            run_info=run_info,
            payload=input,
            return_type=return_type,
        )

        # Attached before the start command goes out so the caller observes the run from
        # its first event; if the server rejects the start, the handle is not left
        # listening to a run that will never exist.
        await handle.attach()
        try:
            await handle.request_start()
        except Exception:
            await handle.detach()
            raise

        return handle
