"""What makes two calls the same call, and what replay does when they are not.

Each operation composes its own id, so each one can independently fail to include an
input that matters. These tests pin, per operation, which inputs are part of its
identity — a divergence left out of an id is one replay will never catch.
"""

import logging
from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.journal import NonDeterminismError, identify
from grctl.exec.operations import Now, Random, SendToParent, Sleep, StartChild, Uuid4
from grctl.models import RunInfo
from grctl.nats.codec import MsgspecCodec
from grctl.serde import SerializerRegistry
from tests.unit.exec.fakes import (
    DEFAULT_RUN_INFO,
    DEFAULT_WORKER_ID,
    FakeAppender,
    FakeHistoryListenerFactory,
    FakeWorkflowAPI,
    make_context,
)

_PARENT_RUN = RunInfo(id="parent-run", wf_id="parent-wf", wf_type="parent-type")


def _start_child(
    workflow_id: str = "child-1", workflow_input: dict | None = None, codec: MsgspecCodec | None = None
) -> StartChild:
    return StartChild(
        DEFAULT_RUN_INFO,
        DEFAULT_WORKER_ID,
        FakeWorkflowAPI(),
        codec if codec is not None else MsgspecCodec(),
        FakeHistoryListenerFactory(),
        logging.getLogger("tests.exec"),
        ChildTracker(),
        "child-workflow",
        workflow_id,
        workflow_input,
    )


def _send_to_parent(event_name: str, payload: object = None, codec: MsgspecCodec | None = None) -> SendToParent:
    return SendToParent(
        _PARENT_RUN,
        DEFAULT_WORKER_ID,
        FakeWorkflowAPI(),
        codec if codec is not None else MsgspecCodec(),
        event_name,
        payload,
    )


# --- identify: the shared format every operation composes through ---


def test_id_without_arguments_stays_plain() -> None:
    assert identify("now", 3) == "now:3"
    assert identify("now", 3, {}) == "now:3"


def test_id_with_arguments_keeps_name_and_position_readable() -> None:
    operation_id = identify("greet", 1, {"value": "hello"})

    assert operation_id.startswith("greet:1:")
    assert operation_id != identify("greet", 1, {"value": "world"})


def test_call_position_separates_otherwise_identical_calls() -> None:
    assert identify("greet", 1, {"value": "hi"}) != identify("greet", 2, {"value": "hi"})


# --- per operation: which inputs are part of its identity ---


def test_operations_without_inputs_identify_by_position_alone() -> None:
    assert Now().operation_id(1) == "now:1"
    assert Random().operation_id(2) == "random:2"
    assert Uuid4().operation_id(3) == "uuid4:3"


def test_sleep_identity_includes_its_duration() -> None:
    assert Sleep(timedelta(seconds=1)).operation_id(1) != Sleep(timedelta(seconds=2)).operation_id(1)
    assert Sleep(timedelta(seconds=1)).operation_id(1) == Sleep(timedelta(milliseconds=1000)).operation_id(1)


def test_start_child_identity_includes_the_child_id() -> None:
    assert _start_child("child-a").operation_id(1) != _start_child("child-b").operation_id(1)


def test_start_child_identity_includes_the_child_input() -> None:
    assert _start_child(workflow_input={"x": 1}).operation_id(1) != _start_child(workflow_input={"x": 2}).operation_id(
        1
    )


def test_send_to_parent_identity_includes_the_event_name() -> None:
    assert _send_to_parent("status_v1").operation_id(1) != _send_to_parent("status_v2").operation_id(1)


def test_send_to_parent_identity_includes_the_payload() -> None:
    assert _send_to_parent("status", {"n": 1}).operation_id(1) != _send_to_parent("status", {"n": 2}).operation_id(1)


class RegisteredPayload:
    def __init__(self, value: str) -> None:
        self.value = value


class RegisteredPayloadSerializer:
    def encode(self, value: RegisteredPayload) -> Any:
        return {"value": value.value}

    def decode(self, raw: Any) -> RegisteredPayload:
        return RegisteredPayload(raw["value"])


class PydanticPayload(BaseModel):
    value: str


@pytest.mark.parametrize("payload", [RegisteredPayload("custom"), PydanticPayload(value="pydantic")])
def test_child_operation_identity_accepts_serde_supported_payloads(payload: object) -> None:
    registry = SerializerRegistry()
    registry.register(RegisteredPayload, RegisteredPayloadSerializer())
    codec = MsgspecCodec(registry)

    assert _start_child(workflow_input={"payload": payload}, codec=codec).operation_id(1)
    assert _send_to_parent("result", payload, codec=codec).operation_id(1)


# A task's identity is observed through replay rather than directly: Task is constructed
# inside Context.run_task, which is also the only way user code ever reaches it.

# --- what replay does when identity diverges ---


async def test_same_arguments_replay_the_recorded_outcome() -> None:
    calls = 0

    async def greet(value: str) -> str:
        nonlocal calls
        calls += 1
        return value.upper()

    appender = FakeAppender()
    ctx = make_context(appender=appender)
    assert await ctx.run(greet, "hello") == "HELLO"
    assert calls == 1

    replay_ctx = make_context(appender.events, appender=FakeAppender())

    assert await replay_ctx.run(greet, "hello") == "HELLO"
    assert calls == 1  # replayed, not re-run


async def test_changed_argument_raises_instead_of_replaying_the_old_outcome() -> None:
    """The point of the whole mechanism: a result computed from another input is not this call's."""

    async def greet(value: str) -> str:
        return value.upper()

    appender = FakeAppender()
    ctx = make_context(appender=appender)
    await ctx.run(greet, "hello")

    replay_ctx = make_context(appender.events, appender=FakeAppender())

    with pytest.raises(NonDeterminismError):
        await replay_ctx.run(greet, "world")


async def test_divergence_message_names_both_sides() -> None:
    async def greet(value: str) -> str:
        return value.upper()

    appender = FakeAppender()
    await make_context(appender=appender).run(greet, "hello")
    recorded_id = appender.events[-1].operation_id

    replay_ctx = make_context(appender.events, appender=FakeAppender())

    with pytest.raises(NonDeterminismError) as raised:
        await replay_ctx.run(greet, "world")

    message = str(raised.value)
    assert recorded_id in message  # what history holds
    assert "greet:1:" in message  # what the code called
    assert "task.completed" in message


async def test_changed_sleep_duration_raises_on_replay() -> None:
    appender = FakeAppender()
    await make_context(appender=appender).sleep(timedelta(milliseconds=1))

    replay_ctx = make_context(appender.events, appender=FakeAppender())

    with pytest.raises(NonDeterminismError):
        await replay_ctx.sleep(timedelta(milliseconds=2))


async def test_changed_send_to_parent_event_name_raises_on_replay() -> None:
    appender = FakeAppender()
    ctx = make_context(appender=appender, parent_run=_PARENT_RUN)
    await ctx.send_to_parent("status_v1")

    replay_ctx = make_context(appender.events, appender=FakeAppender(), parent_run=_PARENT_RUN)

    with pytest.raises(NonDeterminismError):
        await replay_ctx.send_to_parent("status_v2")


async def test_nondeterministic_child_id_raises_instead_of_rebuilding_the_old_handle() -> None:
    """Replaying the recorded payload would hand back a handle to a child never started."""
    appender = FakeAppender()
    ctx = make_context(appender=appender)
    await ctx.start_child("child-workflow", "child-deterministic")

    replay_ctx = make_context(appender.events, appender=FakeAppender())

    with pytest.raises(NonDeterminismError):
        await replay_ctx.start_child("child-workflow", "child-freshly-generated")
