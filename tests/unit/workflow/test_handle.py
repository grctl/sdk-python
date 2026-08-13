import asyncio

import pytest

from grctl.workflow.handle import WorkflowHandle, WorkflowHandleFactory
from tests.unit.workflow.fakes import DEFAULT_RUN_INFO, LOGGER, CapturingListenerFactory, FakeWorkflowAPI


def make_handle(payload: object | None = None) -> tuple[WorkflowHandle, FakeWorkflowAPI, CapturingListenerFactory]:
    workflow_api = FakeWorkflowAPI()
    listener_factory = CapturingListenerFactory()
    handle = WorkflowHandle(
        DEFAULT_RUN_INFO,
        payload,
        workflow_api,
        listener_factory,
        sender_id="worker-1",
        logger=LOGGER,
    )
    return handle, workflow_api, listener_factory


async def test_factory_binds_participant_dependencies_to_each_handle() -> None:
    workflow_api = FakeWorkflowAPI()
    listener_factory = CapturingListenerFactory()
    factory = WorkflowHandleFactory(
        workflow_api=workflow_api,
        listener_factory=listener_factory,
        sender_id="worker-1",
        logger=LOGGER,
    )

    handle = factory.create(DEFAULT_RUN_INFO, payload={"x": 1}, return_type=str)

    assert handle.run_info == DEFAULT_RUN_INFO
    assert handle.future.payload == {"x": 1}
    assert listener_factory.run_info == DEFAULT_RUN_INFO

    await handle.request_start()

    assert workflow_api.calls[0].sender_id == "worker-1"
    assert workflow_api.calls[0].kwargs["input"] == {"x": 1}


async def test_request_start_sends_a_start_command_without_listening() -> None:
    """Asking the server to start a run is separate from observing it.

    A step that starts a child publishes the command as its recorded side effect and
    attaches on a separate path that also runs on replay, so the two cannot be one call.
    """
    handle, workflow_api, listener_factory = make_handle(payload={"x": 1})

    await handle.request_start()

    assert listener_factory.listener.start_calls == 0
    assert len(workflow_api.calls) == 1
    call = workflow_api.calls[0]
    assert call.op == "start_run"
    assert call.run_info == DEFAULT_RUN_INFO
    assert call.kwargs["input"] == {"x": 1}
    assert call.sender_id == "worker-1"


async def test_attach_starts_the_listener_without_sending_a_command() -> None:
    handle, workflow_api, listener_factory = make_handle()

    await handle.attach()

    assert listener_factory.listener.start_calls == 1
    assert workflow_api.calls == []


async def test_detach_stops_the_listener() -> None:
    handle, _, listener_factory = make_handle()
    await handle.attach()

    await handle.detach()

    assert listener_factory.listener.stop_calls == 1


async def test_send_publishes_an_event_command() -> None:
    handle, workflow_api, _ = make_handle()

    await handle.send("approved", {"amount": 5})

    assert len(workflow_api.calls) == 1
    call = workflow_api.calls[0]
    assert call.op == "send_event"
    assert call.run_info == DEFAULT_RUN_INFO
    assert call.kwargs["event_name"] == "approved"
    assert call.kwargs["payload"] == {"amount": 5}


async def test_cancel_publishes_a_cancel_command_with_reason() -> None:
    handle, workflow_api, _ = make_handle()

    await handle.cancel("no longer needed")

    call = workflow_api.calls[0]
    assert call.op == "cancel_run"
    assert call.kwargs["reason"] == "no longer needed"


async def test_terminate_publishes_a_terminate_command_with_reason() -> None:
    handle, workflow_api, _ = make_handle()

    await handle.terminate("force stop")

    call = workflow_api.calls[0]
    assert call.op == "terminate_run"
    assert call.kwargs["reason"] == "force stop"


async def test_result_returns_the_future_value_and_stops_the_listener() -> None:
    handle, _, listener_factory = make_handle()
    listener_factory.create(DEFAULT_RUN_INFO, lambda _: None)  # ensure listener exists before future settles
    handle.future.set_result("done")

    result = await handle.result()

    assert result == "done"
    assert listener_factory.listener.stop_calls == 1


async def test_result_times_out_and_still_stops_the_listener() -> None:
    handle, _, listener_factory = make_handle()

    with pytest.raises(asyncio.TimeoutError):
        await handle.result(timeout=0.01)

    assert listener_factory.listener.stop_calls == 1


async def test_query_is_not_yet_implemented() -> None:
    handle, _, _ = make_handle()

    with pytest.raises(NotImplementedError):
        await handle.query("status")


async def test_update_is_not_yet_implemented() -> None:
    handle, _, _ = make_handle()

    with pytest.raises(NotImplementedError):
        await handle.update("rename", {"name": "new"})
