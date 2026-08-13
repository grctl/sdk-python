import asyncio
from logging import Logger
from typing import Any, Protocol

from grctl.models import RunInfo
from grctl.workflow.future import HistoryListenerFactory, ResultDecoder, WorkflowFuture


class WorkflowAPI(Protocol):
    """Run-scoped calls to the server that a WorkflowHandle makes.

    The handle passes its intent; building/routing the underlying command is the
    implementation's concern.

    Each call raises a WorkflowError if the server rejects it; a normal return
    means the server accepted the request.
    """

    async def start_run(self, run_info: RunInfo, input: Any, sender_id: str) -> None: ...  # noqa: A002
    async def send_event(self, run_info: RunInfo, event_name: str, payload: Any, sender_id: str) -> None: ...
    async def cancel_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None: ...
    async def terminate_run(self, run_info: RunInfo, reason: str | None, sender_id: str) -> None: ...


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
        """Begin observing the run's history, so this handle can settle on its outcome.

        Called exactly once per handle. A handle may attach at any point in a run's
        life, including after it has already finished — the listener delivers the run's
        most recent event on join, and a run's outcome is always its most recent event.
        """
        self._logger.debug("Attaching to workflow %s", self.run_info.wf_id)
        await self.future.start()

    async def detach(self) -> None:
        """Stop observing the run, abandoning this handle's view of it."""
        await self.future.stop()

    async def request_start(self) -> None:
        """Ask the server to start this run. Attaching is the caller's separate decision."""
        self._logger.debug(
            "Publishing start command for wf_type=%s wf_id=%s", self.run_info.wf_type, self.run_info.wf_id
        )
        await self._workflow_api.start_run(self.run_info, self._payload, self._sender_id)

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


class WorkflowHandleFactory:
    """Creates workflow handles for one SDK participant."""

    def __init__(
        self,
        workflow_api: WorkflowAPI,
        listener_factory: HistoryListenerFactory,
        sender_id: str,
        logger: Logger,
        decoder: ResultDecoder | None = None,
    ) -> None:
        self._workflow_api = workflow_api
        self._listener_factory = listener_factory
        self._sender_id = sender_id
        self._logger = logger
        self._decoder = decoder

    def create(
        self,
        run_info: RunInfo,
        payload: Any | None = None,
        return_type: type | None = None,
    ) -> WorkflowHandle:
        return WorkflowHandle(
            run_info=run_info,
            payload=payload,
            workflow_api=self._workflow_api,
            listener_factory=self._listener_factory,
            sender_id=self._sender_id,
            logger=self._logger,
            return_type=return_type,
            decoder=self._decoder,
        )
