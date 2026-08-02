import logging
from datetime import UTC, datetime

import pytest

from grctl.exec.child_tracker import ChildTracker
from grctl.exec.context import Context
from grctl.exec.journal import Journal
from grctl.exec.operations import StartChild
from grctl.models import ChildWorkflowStarted, HistoryEvent, HistoryKind, RunInfo
from tests.unit.exec.fakes import (
    FakeAppender,
    FakeHistoryListenerFactory,
    FakeWorkflowAPI,
    make_context,
    record_then_replay,
)


async def test_send_to_parent_raises_without_a_parent() -> None:
    ctx = make_context()

    with pytest.raises(RuntimeError, match="No parent workflow"):
        await ctx.send_to_parent("approved")


async def test_send_to_parent_publishes_and_records_once() -> None:
    parent_run = RunInfo(id="parent-run", wf_id="parent-wf", wf_type="parent-workflow")
    workflow_apis: list[FakeWorkflowAPI] = []

    def context(step_history: list[HistoryEvent] | None, *, appender: FakeAppender) -> Context:
        workflow_api = FakeWorkflowAPI()
        workflow_apis.append(workflow_api)
        return make_context(step_history, appender=appender, workflow_api=workflow_api, parent_run=parent_run)

    result = await record_then_replay(context, lambda ctx: ctx.send_to_parent("approved", {"amount": 5}))

    record_api, replay_api = workflow_apis
    assert len(record_api.calls) == 1
    assert len(result.events) == 1
    assert result.events[0].kind == HistoryKind.parent_event_sent
    assert replay_api.calls == []


async def test_start_child_replay_reconstructs_handle_without_publishing() -> None:
    """On replay, start_child must not re-publish a start command for the child."""
    run_info = RunInfo(id="run-1", wf_id="wf-1", wf_type="test-workflow")
    workflow_api = FakeWorkflowAPI()
    listener_factory = FakeHistoryListenerFactory()
    op = StartChild(
        run_info,
        "worker-1",
        workflow_api,
        listener_factory,
        logging.getLogger("tests.exec"),
        ChildTracker(),
        "child-workflow",
        "child-1",
        {"x": 1},
    )
    operation_id = Journal(step_history=[], appender=FakeAppender()).generate_operation_id(op.name)

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
        [recorded_event],
        appender=replay_appender,
        workflow_api=workflow_api,
        listener_factory=listener_factory,
        run_info=run_info,
        childs=childs,
    )

    handle = await ctx.start_child("child-workflow", "child-1", {"x": 1})

    assert handle.run_info.id == "child-run-1"
    assert workflow_api.calls == []
    assert replay_appender.events == []
    assert childs.started == [handle]
