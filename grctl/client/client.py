"""Workflow Engine Client.

Provides a simple interface for interacting with workflows.
"""

import logging
import secrets
import socket
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar, overload

import msgspec
from ulid import ULID

from grctl.models import HistoryEvent, RunInfo
from grctl.models.errors import (
    WorkflowAlreadyRunningError,
    WorkflowError,
    WorkflowNotFoundError,
    WorkflowTypeNotRegisteredError,
)
from grctl.nats.connection import Connection
from grctl.workflow.handle import WorkflowHandle

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

ErrWorkflowAlreadyRunningCode = 4001
ErrWorkflowRunNotFoundCode = 4002
ErrWorkflowTypeNotRegisteredCode = 4004


class Client:
    """Client for interacting with the Workflow Engine."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self.id = f"c_{secrets.token_hex(4)}@{socket.gethostname()}"

    async def describe(self, wf_id: str) -> RunInfo:
        """Describe the latest run for a workflow ID."""
        response = await self._connection.workflow_api.describe_run(wf_id, self.id)
        if not response.success:
            error_msg = response.error.message if response.error else "unknown error"
            error_code = response.error.code if response.error else 0
            if error_code == ErrWorkflowRunNotFoundCode:
                raise WorkflowNotFoundError(f"workflow '{wf_id}' not found: {error_msg}")
            raise WorkflowError(f"describe failed (code={error_code}): {error_msg}")

        return msgspec.msgpack.decode(response.payload, type=RunInfo)

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

        handle = WorkflowHandle(
            run_info=run_info,
            payload=None,
            workflow_api=self._connection.workflow_api,
            listener_factory=self._connection.listener_factory,
            sender_id=self.id,
            logger=logger,
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

        handle = WorkflowHandle(
            run_info=run_info,
            payload=input,
            workflow_api=self._connection.workflow_api,
            listener_factory=self._connection.listener_factory,
            sender_id=self.id,
            logger=logger,
            return_type=return_type,
        )

        # Start the workflow future (subscribe to events and publish run command)
        response = await handle.start()
        if not response.success:
            await handle.future.stop()
            error_msg = response.error.message if response.error else "unknown error"
            error_code = response.error.code if response.error else 0
            if error_code == ErrWorkflowAlreadyRunningCode:
                raise WorkflowAlreadyRunningError(f"workflow '{id}' already has an active run: {error_msg}")
            if error_code == ErrWorkflowTypeNotRegisteredCode:
                raise WorkflowTypeNotRegisteredError(f"no worker registered for workflow type '{type}': {error_msg}")
            raise WorkflowError(f"start_workflow failed (code={error_code}): {error_msg}")

        return handle
