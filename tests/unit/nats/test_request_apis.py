import msgspec
import pytest
from nats.client.message import Message

from grctl.models import (
    GrctlAPIError,
    GrctlAPIResponse,
    RunInfo,
)
from grctl.models.errors import ERR_WORKFLOW_ALREADY_RUNNING, WorkflowAlreadyRunningError
from grctl.nats.codec import MsgspecCodec
from grctl.nats.manifest import manifest
from grctl.nats.worker_api import NatsWorkerAPI
from grctl.nats.workflow_api import NatsWorkflowAPI


class FakeCoreClient:
    """Records core request/reply calls and supplies scripted replies."""

    def __init__(self, replies: list[bytes | Exception]) -> None:
        self._replies = iter(replies)
        self.calls: list[tuple[str, bytes, float]] = []

    async def request(self, subject: str, payload: bytes, *, timeout: float) -> Message:  # noqa: ASYNC109
        self.calls.append((subject, payload, timeout))
        reply = next(self._replies)
        if isinstance(reply, Exception):
            raise reply
        return Message(subject="response", data=reply)


def response_bytes(response: GrctlAPIResponse) -> bytes:
    return msgspec.msgpack.encode(response)


async def test_worker_api_retries_a_core_request_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeCoreClient(
        [
            TimeoutError("request timed out"),
            response_bytes(GrctlAPIResponse(success=True)),
        ]
    )
    api = NatsWorkerAPI(client, MsgspecCodec())
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("grctl.nats.worker_api.asyncio.sleep", record_sleep)

    response = await api.register_worker("worker-1", [])

    assert response.success is True
    assert [subject for subject, _, _ in client.calls] == [manifest.worker_command_subject()] * 2
    assert sleeps == [0.5]


async def test_workflow_api_translates_a_server_rejection() -> None:
    client = FakeCoreClient(
        [
            response_bytes(
                GrctlAPIResponse(
                    success=False,
                    error=GrctlAPIError(code=ERR_WORKFLOW_ALREADY_RUNNING, message="workflow already running"),
                )
            )
        ]
    )
    api = NatsWorkflowAPI(client, MsgspecCodec())
    run_info = RunInfo(id="run-1", wf_id="workflow-1", wf_type="example")

    with pytest.raises(WorkflowAlreadyRunningError, match="workflow already running"):
        await api.start_run(run_info, {"input": "value"}, "client-1")

    subject, _, timeout = client.calls[0]
    assert subject == manifest.api_subject(run_info.wf_id)
    assert timeout == 5.0
