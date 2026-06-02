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

from grctl.models import DescribeCmd, GrctlAPIResponse, HistoryEvent, RunInfo
from grctl.models.command import CmdKind, Command
from grctl.models.errors import WorkflowAlreadyRunningError, WorkflowError, WorkflowNotFoundError
from grctl.nats.connection import Connection
from grctl.nats.history_fetch import fetch_run_history
from grctl.worker.codec import CodecRegistry
from grctl.workflow.handle import WorkflowHandle

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

ErrWorkflowAlreadyRunningCode = 4001
ErrWorkflowRunNotFoundCode = 4002


class Client:
    """Client for interacting with the Workflow Engine."""

    def __init__(self, connection: Connection, codec: CodecRegistry | None = None) -> None:
        self._connection = connection
        self._codec = codec or CodecRegistry()
        self.id = f"c_{secrets.token_hex(4)}@{socket.gethostname()}"

    async def describe(self, wf_id: str) -> RunInfo:
        """Describe the latest run for a workflow ID."""
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_describe,
            timestamp=datetime.now(UTC),
            msg=DescribeCmd(wf_id=wf_id),
            sender_id=self.id,
        )
        # Use a routing-only RunInfo — publish_cmd only needs wf_id for subject routing.
        routing_info = RunInfo(id="", wf_type="", wf_id=wf_id)
        response_bytes = await self._connection.publisher.publish_cmd(routing_info, cmd)

        response = msgspec.msgpack.decode(response_bytes, type=GrctlAPIResponse)
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
            connection=self._connection,
            codec=self._codec,
            sender_id=self.id,
        )
        await handle.attach()
        return handle

    async def get_history(self, wf_id: str, run_id: str | None = None) -> list[HistoryEvent]:
        """Return the ordered history events for a workflow run."""
        resolved_run_id = run_id
        if resolved_run_id is None:
            resolved_run_id = (await self.describe(wf_id)).id

        return await fetch_run_history(
            js=self._connection.js,
            manifest=self._connection.manifest,
            wf_id=wf_id,
            run_id=resolved_run_id,
        )

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
            connection=self._connection,
            codec=self._codec,
            return_type=return_type,
            sender_id=self.id,
        )

        # Start the workflow future (subscribe to events and publish run command)
        response_bytes = await handle.start()
        response = msgspec.msgpack.decode(response_bytes, type=GrctlAPIResponse)
        if not response.success:
            await handle.future.stop()
            error_msg = response.error.message if response.error else "unknown error"
            error_code = response.error.code if response.error else 0
            if error_code == ErrWorkflowAlreadyRunningCode:
                raise WorkflowAlreadyRunningError(f"workflow '{id}' already has an active run: {error_msg}")
            raise WorkflowError(f"start_workflow failed (code={error_code}): {error_msg}")

        return handle
