import asyncio
from logging import Logger
from typing import Any, Protocol

from grctl.models import GrctlAPIResponse, RunInfo
from grctl.workflow.future import HistoryListenerFactory, ResultDecoder, WorkflowFuture


class WorkflowAPI(Protocol):
    """Run-scoped calls to the server that a WorkflowHandle makes.

    The handle passes its intent; building/routing the underlying command is the
    implementation's concern.
    """

    async def start_run(self, run_info: RunInfo, input: Any, sender_id: str) -> GrctlAPIResponse: ...  # noqa: A002
    async def send_event(
        self, run_info: RunInfo, event_name: str, payload: Any, sender_id: str
    ) -> GrctlAPIResponse: ...
    async def cancel_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> GrctlAPIResponse: ...
    async def terminate_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> GrctlAPIResponse: ...


class WorkflowHandle:
    def __init__(  # noqa: PLR0913
        self,
        run_info: RunInfo,
        payload: Any | None,
        workflow_api: WorkflowAPI,
        listener_factory: HistoryListenerFactory,
        sender_id: str,
        logger: Logger,
        return_type: type | None = None,
        decoder: ResultDecoder | None = None,
    ) -> None:
        self.run_info = run_info
        self._payload = payload
        self._workflow_api = workflow_api
        self._sender_id = sender_id
        self._logger = logger
        self.future = WorkflowFuture(
            run_info, listener_factory, logger, payload, return_type=return_type, decoder=decoder
        )

    async def attach(self) -> None:
        """Attach to an existing workflow run by starting the future subscription only."""
        self._logger.debug("Attaching to existing workflow %s", self.run_info.wf_id)
        await self.future.start()

    async def start(self) -> GrctlAPIResponse:
        """Start the workflow future (subscribe to events and publish run command)."""
        self._logger.debug("Starting workflow history listener")
        await self.future.start()
        self._logger.debug(
            "Publishing start command for wf_type=%s wf_id=%s", self.run_info.wf_type, self.run_info.wf_id
        )
        return await self._workflow_api.start_run(self.run_info, self._payload, self._sender_id)

    async def send(self, event_name: str, payload: Any | None = None) -> None:
        self._logger.debug("Sending event '%s' to workflow %s", event_name, self.run_info.wf_id)
        await self._workflow_api.send_event(self.run_info, event_name, payload, self._sender_id)

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
        await self._workflow_api.cancel_run(self.run_info, reason, self._sender_id)

    async def terminate(self, reason: str | None = None) -> None:
        await self._workflow_api.terminate_run(self.run_info, reason, self._sender_id)

    async def query(self, query_name: str) -> Any:
        raise NotImplementedError("query() not yet implemented")

    async def update(self, update_name: str, data: Any) -> Any:
        raise NotImplementedError("update() not yet implemented")
