import asyncio
import time
from datetime import timedelta

import ulid

from grctl.models import HistoryKind
from grctl.models.history import StepStarted
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess
from tests.spec.workflows import unique_workflow_type


def _make_timeout_workflow() -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type("spec_event_timeout"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait(
            timeout=timedelta(milliseconds=100),
            on_timeout=on_timeout,
        )

    @wf.step()
    async def on_timeout(ctx: Context) -> Directive:
        return ctx.next.complete("timed_out")

    return wf


async def test_wait_timeout_runs_timeout_step(worker, grctl_client) -> None:
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
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=10.0)

        deadline = time.monotonic() + 10.0
        timeout_step = None
        while time.monotonic() < deadline:
            for event in await history.events():
                if (
                    event.kind == HistoryKind.step_started
                    and isinstance(event.msg, StepStarted)
                    and event.msg.step_name == "on_timeout"
                ):
                    timeout_step = event
                    break
            if timeout_step is not None:
                break
            await asyncio.sleep(0.1)

        assert timeout_step is not None, f"Timed out waiting for on_timeout step.started — wf_id={wf_id}"
        assert timeout_step.msg.step_name == "on_timeout"  # ty:ignore[unresolved-attribute]
    finally:
        await handle.future.stop()


async def test_wait_timeout_step_completes_workflow(worker, grctl_client) -> None:
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
        result = await asyncio.wait_for(handle.future, timeout=10)
        assert result == "timed_out"

        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id)
        events = await history.events()
        assert any(e.kind == HistoryKind.run_completed for e in events)
    finally:
        await handle.future.stop()
