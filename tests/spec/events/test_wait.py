import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowError
from grctl.models.history import EventReceived
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_waiting_event_workflow, unique_workflow_type


def _make_timeout_workflow(has_timeout_handler: bool = False) -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type("spec_event_timeout"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        if has_timeout_handler:
            return ctx.next.wait(timeout=timedelta(milliseconds=100), on_timeout=on_timeout)
        return ctx.next.wait(timeout=timedelta(milliseconds=100))

    @wf.step()
    async def on_timeout(ctx: Context) -> Directive:
        return ctx.next.complete("timed_out")

    return wf


async def test_wait_emits_wait_started(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_event_wait_emits")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        event, _ = await history.wait_for_kind(HistoryKind.wait_started)
        assert event.kind == HistoryKind.wait_started
    finally:
        await handle.future.stop()


async def test_workflow_resumes_after_event_received(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_event_wait_resume")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        await history.wait_for_kind(HistoryKind.wait_started)
        await handle.send("finish")

        event, all_events = await history.wait_for_kind(HistoryKind.event_received)
        assert isinstance(event.msg, EventReceived)
        assert event.msg.event_name == "finish"

        kinds = [e.kind for e in all_events]
        wait_idx = kinds.index(HistoryKind.wait_started)
        recv_idx = kinds.index(HistoryKind.event_received)
        assert wait_idx < recv_idx

        result = await asyncio.wait_for(handle.future, timeout=30)
        assert result == "done"
    finally:
        await handle.future.stop()


async def test_wait_timeout_runs_timeout_step(worker, grctl_client) -> None:
    wf = _make_timeout_workflow(has_timeout_handler=True)
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        result = await asyncio.wait_for(handle.future, timeout=10)
        assert result == "timed_out"

        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        events = await history.events()
        assert any(e.kind == HistoryKind.run_completed for e in events)
    finally:
        await handle.future.stop()


async def test_wait_timeout_without_handler_fails_workflow(worker, grctl_client) -> None:
    wf = _make_timeout_workflow()
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        with pytest.raises(WorkflowError, match="wait timed out without"):
            await asyncio.wait_for(handle.future, timeout=15)

        await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run(
            [HistoryKind.run_started, HistoryKind.run_failed]
        )
    finally:
        await handle.future.stop()
