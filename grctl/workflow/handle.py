import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar, overload

from ulid import ULID

from grctl.logging_config import get_logger
from grctl.models import CancelCmd, CmdKind, Command, EventCmd, RunInfo, StartCmd
from grctl.worker.codec import CodecRegistry
from grctl.workflow.future import WorkflowFuture

if TYPE_CHECKING:
    from grctl.nats.connection import Connection

logger = get_logger(__name__)

_T = TypeVar("_T")


class WorkflowHandle:
    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        payload: Any | None,
        connection: "Connection",
        sender_id: str,
        codec: CodecRegistry | None = None,
        return_type: type | None = None,
    ) -> None:
        self.run_info = run_info
        self._payload = payload
        self._connection = connection
        self._codec = codec or CodecRegistry()
        self._return_type = return_type
        self._sender_id = sender_id
        self.future = WorkflowFuture(run_info, connection.nc, payload)

    async def attach(self) -> None:
        """Attach to an existing workflow run by starting the future subscription only."""
        logger.debug("Attaching to existing workflow %s", self.run_info.wf_id)
        await self.future.start()

    async def start(self) -> bytes:
        """Start the workflow future (subscribe to events and publish run command)."""
        input_value = self._codec.decode(self._codec.encode(self._payload)) if self._payload is not None else None
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_start,
            timestamp=datetime.now(UTC),
            msg=StartCmd(
                run_info=self.run_info,
                input=input_value,
            ),
            sender_id=self._sender_id,
        )
        logger.debug("Starting workflow history listener")
        await self.future.start()
        logger.debug("Publishing start command for workflow %s ", cmd)
        return await self._connection.publisher.publish_cmd(self.run_info, cmd)

    async def send(self, event_name: str, payload: Any | None = None) -> None:
        normalized = self._codec.decode(self._codec.encode(payload)) if payload is not None else None
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_event,
            timestamp=datetime.now(UTC),
            msg=EventCmd(
                wf_id=self.run_info.wf_id,
                event_name=event_name,
                payload=normalized,
            ),
            sender_id=self._sender_id,
        )
        logger.debug("Publishing event command for workflow %s", cmd)
        await self._connection.publisher.publish_cmd(self.run_info, cmd)

    @overload
    async def result(self, timeout: float | None = ..., return_type: type[_T] = ...) -> _T: ...  # noqa: ASYNC109

    @overload
    async def result(self, timeout: float | None = ..., return_type: None = ...) -> Any: ...  # noqa: ASYNC109

    async def result(
        self,
        timeout: float | None = None,  # noqa: ASYNC109
        return_type: type[_T] | None = None,
    ) -> _T | Any:
        """Wait for workflow completion and return its result.

        timeout: client-side wait in seconds, independent of any server-side execution timeout.
        return_type: overrides the type bound at start time; falls back to handle's bound type.
        """
        resolved_type = return_type or self._return_type
        try:
            raw = await asyncio.wait_for(self.future, timeout=timeout)
            if resolved_type is not None:
                return self._codec.from_primitive(raw, resolved_type)
            return raw
        finally:
            await self.future.stop()

    async def cancel(self, reason: str | None = None) -> None:
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_cancel,
            timestamp=datetime.now(UTC),
            msg=CancelCmd(
                wf_id=self.run_info.wf_id,
                reason=reason,
            ),
            sender_id=self._sender_id,
        )
        await self._connection.publisher.publish_cmd(self.run_info, cmd)

    async def query(self, query_name: str) -> Any:
        raise NotImplementedError("query() not yet implemented")

    async def update(self, update_name: str, data: Any) -> Any:
        raise NotImplementedError("update() not yet implemented")
