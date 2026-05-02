"""Workflow Engine Client.

Provides a simple interface for interacting with workflows.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import msgspec
from ulid import ULID

from grctl.models import DescribeCmd, GrctlAPIResponse, RunInfo
from grctl.models.command import CmdKind, Command
from grctl.models.errors import WorkflowError, WorkflowNotFoundError
from grctl.nats.connection import Connection
from grctl.worker.codec import CodecRegistry
from grctl.workflow.handle import WorkflowHandle

logger = logging.getLogger(__name__)

ErrWorkflowRunNotFoundCode = 4002


class Client:
    """Client for interacting with the Workflow Engine."""

    def __init__(self, connection: Connection, codec: CodecRegistry | None = None) -> None:
        self._connection = connection
        self._codec = codec or CodecRegistry()

    async def run_workflow(
        self,
        type: str,  # noqa: A002
        id: str,  # noqa: A002
        input: Any | None = None,  # noqa: A002
        timeout: timedelta | None = None,  # noqa: ASYNC109
    ) -> Any:
        """Run a workflow and wait for its result."""
        wf_handle = await self.start_workflow(
            type=type,
            id=id,
            input=input,
            timeout=timeout,
        )
        wait_timeout = timeout.total_seconds() if timeout else None
        try:
            return await asyncio.wait_for(wf_handle.future, timeout=wait_timeout)
        finally:
            await wf_handle.future.stop()

    async def get_workflow_handle(self, workflow_id: str) -> WorkflowHandle:
        """Get a handle for an already-running workflow."""
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_describe,
            timestamp=datetime.now(UTC),
            msg=DescribeCmd(wf_id=workflow_id),
        )
        # Use a routing-only RunInfo — publish_cmd only needs wf_id for subject routing.
        routing_info = RunInfo(id="", wf_type="", wf_id=workflow_id)
        response_bytes = await self._connection.publisher.publish_cmd(routing_info, cmd)

        response = msgspec.msgpack.decode(response_bytes, type=GrctlAPIResponse)
        if not response.success:
            error_msg = response.error.message if response.error else "unknown error"
            error_code = response.error.code if response.error else 0
            if error_code == ErrWorkflowRunNotFoundCode:
                raise WorkflowNotFoundError(f"workflow '{workflow_id}' not found: {error_msg}")
            raise WorkflowError(f"describe failed (code={error_code}): {error_msg}")

        run_info = msgspec.msgpack.decode(response.payload, type=RunInfo)

        handle = WorkflowHandle(
            run_info=run_info,
            payload=None,
            connection=self._connection,
            codec=self._codec,
        )
        await handle.attach()
        return handle

    async def start_workflow(
        self,
        type: str,  # noqa: A002
        id: str,  # noqa: A002
        input: Any | None = None,  # noqa: A002
        timeout: timedelta | None = None,  # noqa: ASYNC109
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
        )

        # Start the workflow future (subscribe to events and publish run command)
        await handle.start()
        return handle
