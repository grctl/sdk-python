from datetime import UTC, datetime
from typing import cast

import pytest

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.context import Context
from grctl.exec.journal import Journal
from grctl.exec.operations import StartChild
from grctl.models import ChildWorkflowStarted, HistoryEvent, HistoryKind, RunInfo
from grctl.nats.connection import Connection
from tests.unit.exec.fakes import FakeAppender, FakeConnection, make_context, record_then_replay


async def test_send_to_parent_raises_without_a_parent() -> None:
    ctx = make_context()

    with pytest.raises(RuntimeError, match="No parent workflow"):
        await ctx.send_to_parent("approved")


async def test_send_to_parent_publishes_and_records_once() -> None:
    parent_run = RunInfo(id="parent-run", wf_id="parent-wf", wf_type="parent-workflow")
    connections: list[FakeConnection] = []

    def context(step_history: list[HistoryEvent] | None, *, appender: FakeAppender) -> Context:
        connection = FakeConnection()
        connections.append(connection)
        return make_context(step_history, appender=appender, connection=connection, parent_run=parent_run)

    result = await record_then_replay(context, lambda ctx: ctx.send_to_parent("approved", {"amount": 5}))

    record_connection, replay_connection = connections
    assert len(record_connection.publisher.published) == 1
    assert len(result.events) == 1
    assert result.events[0].kind == HistoryKind.parent_event_sent
    assert replay_connection.publisher.published == []


async def test_start_child_replay_reconstructs_handle_without_publishing() -> None:
    """On replay, start_child must not re-publish a start command for the child."""
    run_info = RunInfo(id="run-1", wf_id="wf-1", wf_type="test-workflow")
    connection = FakeConnection()
    op = StartChild(
        run_info, "worker-1", cast("Connection", connection), ChildTracker(), "child-workflow", "child-1", {"x": 1}
    )
    operation_id = Journal(step_history=[], appender=FakeAppender()).generate_operation_id(op.name, op.args)

    recorded_event = HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.child_started,
        msg=ChildWorkflowStarted(run_id="child-run-1", wf_type="child-workflow", wf_id="child-1", input={"x": 1}),
        operation_id=operation_id,
    )

    replay_appender = FakeAppender()
    childs = ChildTracker()
    ctx = make_context(
        [recorded_event], appender=replay_appender, connection=connection, run_info=run_info, childs=childs
    )

    handle = await ctx.start_child("child-workflow", "child-1", {"x": 1})

    assert handle.run_info.id == "child-run-1"
    assert connection.publisher.published == []
    assert replay_appender.events == []
    assert childs.started == [handle]
