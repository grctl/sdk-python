import asyncio
import time
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowError
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess
from tests.spec.workflows import unique_workflow_type


async def test_multiple_event_types_dispatch_to_correct_handlers(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_event_dispatch_multi"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait()

    @wf.event()
    async def approve(ctx: Context) -> Directive:
        return ctx.next.complete("approve")

    @wf.event()
    async def reject(ctx: Context) -> Directive:
        return ctx.next.complete("reject")

    await worker([wf])

    wf_id_approve = str(ulid.ULID())
    handle_approve = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id_approve,
        input={},
        timeout=timedelta(seconds=30),
    )

    wf_id_reject = str(ulid.ULID())
    handle_reject = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id_reject,
        input={},
        timeout=timedelta(seconds=30),
    )

    try:
        history_approve = HistoryAccess(grctl_client, wf_id_approve, handle_approve.run_info.id)
        await history_approve.wait_for_kind(HistoryKind.wait_started)
        await handle_approve.send("approve")
        result_approve = await asyncio.wait_for(handle_approve.future, timeout=30)
        assert result_approve == "approve"

        history_reject = HistoryAccess(grctl_client, wf_id_reject, handle_reject.run_info.id)
        await history_reject.wait_for_kind(HistoryKind.wait_started)
        await handle_reject.send("reject")
        result_reject = await asyncio.wait_for(handle_reject.future, timeout=30)
        assert result_reject == "reject"
    finally:
        await handle_approve.future.stop()
        await handle_reject.future.stop()


async def test_event_handler_can_loop_back_to_wait(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_event_dispatch_loopback"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait()

    @wf.event()
    async def first_event(ctx: Context) -> Directive:
        return ctx.next.wait()

    @wf.event()
    async def second_event(ctx: Context) -> Directive:
        return ctx.next.complete("done-after-second")

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
        await handle.send("first_event")

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            events = await history.events()
            if sum(1 for e in events if e.kind == HistoryKind.wait_started) >= 2:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError(f"Timed out waiting for second wait.started — wf_id={wf_id}")

        await handle.send("second_event")
        result = await asyncio.wait_for(handle.future, timeout=30)
        assert result == "done-after-second"
    finally:
        await handle.future.stop()


async def test_event_timeout_fails_workflow(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=unique_workflow_type("spec_dispatch_event_timeout_with_handler"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait()

    @wf.event(timeout=timedelta(milliseconds=100))
    async def finish(ctx: Context) -> Directive:
        await asyncio.sleep(1)
        return ctx.next.complete("done")

    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    await handle.send("finish")

    try:
        with pytest.raises(WorkflowError, match="step finish timed out"):
            await asyncio.wait_for(handle.future, timeout=15)
    finally:
        await handle.future.stop()
