import asyncio
from datetime import UTC, datetime
from logging import Logger
from typing import Any, Protocol

from ulid import ULID

from grctl.models import CancelCmd, CmdKind, Command, EventCmd, RunInfo, StartCmd, TerminateCmd
from grctl.workflow.future import HistoryListenerFactory, ResultDecoder, WorkflowFuture


class CommandSender(Protocol):
    """Delivers a Command to a run and returns the raw response."""

    async def send(self, run: RunInfo, cmd: Command) -> bytes: ...


class WorkflowHandle:
    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        payload: Any | None,
        command_sender: CommandSender,
        listener_factory: HistoryListenerFactory,
        sender_id: str,
        logger: Logger,
        return_type: type | None = None,
        decoder: ResultDecoder | None = None,
    ) -> None:
        self.run_info = run_info
        self._payload = payload
        self._command_sender = command_sender
        self._sender_id = sender_id
        self._logger = logger
        self.future = WorkflowFuture(
            run_info, listener_factory, logger, payload, return_type=return_type, decoder=decoder
        )

    async def attach(self) -> None:
        """Attach to an existing workflow run by starting the future subscription only."""
        self._logger.debug("Attaching to existing workflow %s", self.run_info.wf_id)
        await self.future.start()

    async def start(self) -> bytes:
        """Start the workflow future (subscribe to events and publish run command)."""
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_start,
            timestamp=datetime.now(UTC),
            msg=StartCmd(
                run_info=self.run_info,
                input=self._payload,
            ),
            sender_id=self._sender_id,
        )
        self._logger.debug("Starting workflow history listener")
        await self.future.start()
        self._logger.debug(
            "Publishing start command for wf_type=%s wf_id=%s", self.run_info.wf_type, self.run_info.wf_id
        )
        return await self._command_sender.send(self.run_info, cmd)

    async def send(self, event_name: str, payload: Any | None = None) -> None:
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_event,
            timestamp=datetime.now(UTC),
            msg=EventCmd(
                wf_id=self.run_info.wf_id,
                event_name=event_name,
                payload=payload,
            ),
            sender_id=self._sender_id,
        )
        self._logger.debug("Publishing event command for workflow %s", cmd)
        await self._command_sender.send(self.run_info, cmd)

    async def result(self, timeout: float | None = None) -> Any:  # noqa: ASYNC109
        """Wait for workflow completion and return its result.

        timeout: client-side wait in seconds, independent of any server-side execution timeout.
        The result is already decoded to the return_type bound at construction, if any.
        """
        try:
            return await asyncio.wait_for(self.future, timeout=timeout)
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
        await self._command_sender.send(self.run_info, cmd)

    async def terminate(self, reason: str | None = None) -> None:
        cmd = Command(
            id=str(ULID()),
            kind=CmdKind.run_terminate,
            timestamp=datetime.now(UTC),
            msg=TerminateCmd(
                wf_id=self.run_info.wf_id,
                reason=reason,
            ),
            sender_id=self._sender_id,
        )
        await self._command_sender.send(self.run_info, cmd)

    async def query(self, query_name: str) -> Any:
        raise NotImplementedError("query() not yet implemented")

    async def update(self, update_name: str, data: Any) -> Any:
        raise NotImplementedError("update() not yet implemented")
