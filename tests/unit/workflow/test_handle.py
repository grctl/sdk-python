import asyncio

import pytest

from grctl.models import CancelCmd, CmdKind, EventCmd, StartCmd, TerminateCmd
from grctl.workflow.handle import WorkflowHandle
from tests.unit.workflow.fakes import DEFAULT_RUN_INFO, LOGGER, CapturingListenerFactory, FakeCommandSender


def make_handle(payload: object | None = None) -> tuple[WorkflowHandle, FakeCommandSender, CapturingListenerFactory]:
    command_sender = FakeCommandSender()
    listener_factory = CapturingListenerFactory()
    handle = WorkflowHandle(
        DEFAULT_RUN_INFO,
        payload,
        command_sender,
        listener_factory,
        sender_id="worker-1",
        logger=LOGGER,
    )
    return handle, command_sender, listener_factory


async def test_start_starts_the_future_and_sends_a_start_command() -> None:
    handle, command_sender, listener_factory = make_handle(payload={"x": 1})

    await handle.start()

    assert listener_factory.listener.start_calls == 1
    assert len(command_sender.sent) == 1
    cmd = command_sender.sent[0]
    assert cmd.kind == CmdKind.run_start
    assert isinstance(cmd.msg, StartCmd)
    assert cmd.msg.run_info == DEFAULT_RUN_INFO
    assert cmd.msg.input == {"x": 1}
    assert cmd.sender_id == "worker-1"


async def test_attach_starts_the_future_without_sending_a_command() -> None:
    handle, command_sender, listener_factory = make_handle()

    await handle.attach()

    assert listener_factory.listener.start_calls == 1
    assert command_sender.sent == []


async def test_send_publishes_an_event_command() -> None:
    handle, command_sender, _ = make_handle()

    await handle.send("approved", {"amount": 5})

    assert len(command_sender.sent) == 1
    cmd = command_sender.sent[0]
    assert cmd.kind == CmdKind.run_event
    assert isinstance(cmd.msg, EventCmd)
    assert cmd.msg.wf_id == DEFAULT_RUN_INFO.wf_id
    assert cmd.msg.event_name == "approved"
    assert cmd.msg.payload == {"amount": 5}


async def test_cancel_publishes_a_cancel_command_with_reason() -> None:
    handle, command_sender, _ = make_handle()

    await handle.cancel("no longer needed")

    cmd = command_sender.sent[0]
    assert cmd.kind == CmdKind.run_cancel
    assert isinstance(cmd.msg, CancelCmd)
    assert cmd.msg.reason == "no longer needed"


async def test_terminate_publishes_a_terminate_command_with_reason() -> None:
    handle, command_sender, _ = make_handle()

    await handle.terminate("force stop")

    cmd = command_sender.sent[0]
    assert cmd.kind == CmdKind.run_terminate
    assert isinstance(cmd.msg, TerminateCmd)
    assert cmd.msg.reason == "force stop"


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
