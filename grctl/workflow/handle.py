from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ulid import ULID

from grctl.logging_config import get_logger
from grctl.models import CmdKind, Command, EventCmd, RunInfo, StartCmd
from grctl.worker.codec import CodecRegistry
from grctl.workflow.future import WorkflowFuture

if TYPE_CHECKING:
    from grctl.nats.connection import Connection

logger = get_logger(__name__)


class WorkflowHandle:
    def __init__(
        self,
        run_info: RunInfo,
        payload: Any | None,
        connection: "Connection",
        codec: CodecRegistry | None = None,
    ) -> None:
        self.run_info = run_info
        self._payload = payload
        self._connection = connection
        self._codec = codec or CodecRegistry()
        self.future = WorkflowFuture(run_info, connection.nc, payload)

    async def attach(self) -> None:
        """Attach to an existing workflow run by starting the future subscription only."""
        logger.debug("Attaching to existing workflow %s", self.run_info.wf_id)
        await self.future.start()

    async def start(self) -> None:
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
        )
        logger.debug("Starting workflow history listener")
        await self.future.start()
        logger.debug("Publishing start command for workflow %s ", cmd)
        await self._connection.publisher.publish_cmd(self.run_info, cmd)

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
        )
        logger.debug("Publishing event command for workflow %s", cmd)
        await self._connection.publisher.publish_cmd(self.run_info, cmd)

    async def query(self, query_name: str) -> Any:
        raise NotImplementedError("query() not yet implemented")

    async def update(self, update_name: str, data: Any) -> Any:
        raise NotImplementedError("update() not yet implemented")
