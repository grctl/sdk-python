import asyncio
import multiprocessing
import os
from datetime import timedelta

import pytest
import ulid

from grctl.client import Client
from grctl.models import ChildWorkflowStarted, HistoryKind
from grctl.models.errors import WorkflowError
from grctl.nats.connection import Connection
from grctl.worker import Context
from grctl.worker.worker import Worker
from grctl.workflow import Directive, Workflow
from tests.spec.history import HistoryAccess
from tests.spec.workflows import unique_workflow_type

_WORKER_INIT_DELAY = 0.5
_HISTORY_TIMEOUT = 15.0
_WORKFLOW_TIMEOUT = timedelta(seconds=120)
_REPLAY_WORKER_ACK_WAIT_SECONDS = "0.5"


def _terminate(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=5.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=1.0)


def _configure_fast_replay_redelivery() -> None:
    os.environ.setdefault("ENGINE_NATS_WORKER_ACK_WAIT", _REPLAY_WORKER_ACK_WAIT_SECONDS)


# ─── ctx.start() skip scenario ────────────────────────────────────────────────


def _ctx_start_replay_worker(parent_wf_type: str, child_wf_type: str, pause_event=None) -> None:
    """Worker for ctx.start() replay test.

    Worker A calls ctx.start() and records child.started, then pauses.
    Worker B picks up the re-delivered directive and skips child startup via replay.
    """

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        child_wf = Workflow(workflow_type=child_wf_type)
        parent_wf = Workflow(workflow_type=parent_wf_type)

        @child_wf.start()
        async def child_start(ctx: Context) -> Directive:
            return ctx.next.complete("child-done")

        @parent_wf.start()
        async def parent_start(ctx: Context) -> Directive:
            return ctx.next.step(parent_main)

        @parent_wf.step()
        async def parent_main(ctx: Context) -> Directive:
            await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete("parent-done")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[parent_wf, child_wf], connection=conn)
        await wk.start()

    asyncio.run(run())


# ─── send_to_parent() skip scenario ───────────────────────────────────────────


def _send_to_parent_replay_worker(parent_wf_type: str, child_wf_type: str, pause_event=None) -> None:
    """Worker for send_to_parent() replay test.

    Worker A calls send_to_parent() and records parent.event_sent, then pauses.
    Worker B picks up the re-delivered directive and skips the send via replay.
    """

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        child_wf = Workflow(workflow_type=child_wf_type)
        parent_wf = Workflow(workflow_type=parent_wf_type)

        @child_wf.start()
        async def child_start(ctx: Context) -> Directive:
            return ctx.next.step(child_send)

        @child_wf.step()
        async def child_send(ctx: Context) -> Directive:
            await ctx.send_to_parent("child_done")
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete("child-done")

        @parent_wf.start()
        async def parent_start(ctx: Context) -> Directive:
            await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
            return ctx.next.wait_for_event(timeout=timedelta(seconds=30))

        @parent_wf.event(name="child_done")
        async def on_child_done(ctx: Context) -> Directive:
            return ctx.next.complete("parent-done")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[parent_wf, child_wf], connection=conn)
        await wk.start()

    asyncio.run(run())


# ─── nondeterministic send_to_parent event name scenario ──────────────────────


def _ndet_send_event_v1_worker(parent_wf_type: str, child_wf_type: str, pause_event=None) -> None:
    """Worker A: child sends event 'status_v1' to parent, then pauses."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        child_wf = Workflow(workflow_type=child_wf_type)
        parent_wf = Workflow(workflow_type=parent_wf_type)

        @child_wf.start()
        async def child_start(ctx: Context) -> Directive:
            return ctx.next.step(child_send)

        @child_wf.step()
        async def child_send(ctx: Context) -> Directive:
            await ctx.send_to_parent("status_v1")
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete("sent")

        @parent_wf.start()
        async def parent_start(ctx: Context) -> Directive:
            await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
            return ctx.next.wait_for_event(timeout=timedelta(seconds=30))

        @parent_wf.event(name="status_v1")
        async def on_status_v1(ctx: Context) -> Directive:
            return ctx.next.complete("v1")

        @parent_wf.event(name="status_v2")
        async def on_status_v2(ctx: Context) -> Directive:
            return ctx.next.complete("v2")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[parent_wf, child_wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _ndet_send_event_v2_worker(parent_wf_type: str, child_wf_type: str) -> None:
    """Worker B: child sends 'status_v2' instead — diverges from history → NonDeterminismError."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        child_wf = Workflow(workflow_type=child_wf_type)
        parent_wf = Workflow(workflow_type=parent_wf_type)

        @child_wf.start()
        async def child_start(ctx: Context) -> Directive:
            return ctx.next.step(child_send)

        @child_wf.step()
        async def child_send(ctx: Context) -> Directive:
            await ctx.send_to_parent("status_v2")  # different event name: diverges from history
            return ctx.next.complete("sent")

        @parent_wf.start()
        async def parent_start(ctx: Context) -> Directive:
            await ctx.start(child_wf_type, f"{ctx.run.wf_id}-child")
            return ctx.next.wait_for_event(timeout=timedelta(seconds=30))

        @parent_wf.event(name="status_v1")
        async def on_status_v1(ctx: Context) -> Directive:
            return ctx.next.complete("v1")

        @parent_wf.event(name="status_v2")
        async def on_status_v2(ctx: Context) -> Directive:
            return ctx.next.complete("v2")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[parent_wf, child_wf], connection=conn)
        await wk.start()

    asyncio.run(run())


# ─── nondeterministic child ID scenario ───────────────────────────────────────


def _ndet_child_id_v1_worker(parent_wf_type: str, child_wf_type: str, pause_event=None) -> None:
    """Worker A: starts child with deterministic ID, records child.started, then pauses."""

    async def run() -> None:
        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        child_wf = Workflow(workflow_type=child_wf_type)
        parent_wf = Workflow(workflow_type=parent_wf_type)

        @child_wf.start()
        async def child_start(ctx: Context) -> Directive:
            return ctx.next.complete("child-done")

        @parent_wf.start()
        async def parent_start(ctx: Context) -> Directive:
            return ctx.next.step(parent_main)

        @parent_wf.step()
        async def parent_main(ctx: Context) -> Directive:
            child_id = f"{ctx.run.wf_id}-child"
            await ctx.start(child_wf_type, child_id)
            if pause_event is not None:
                await asyncio.to_thread(pause_event.wait)
            return ctx.next.complete("done")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[parent_wf, child_wf], connection=conn)
        await wk.start()

    asyncio.run(run())


def _ndet_child_id_v2_worker(parent_wf_type: str, child_wf_type: str) -> None:
    """Worker B: starts child with a new ULID each time — diverges from history → NonDeterminismError."""

    async def run() -> None:
        import ulid as _ulid  # noqa: PLC0415

        _configure_fast_replay_redelivery()
        nats_url = os.environ.get("SPEC_NATS_URL", "nats://localhost:4225")

        child_wf = Workflow(workflow_type=child_wf_type)
        parent_wf = Workflow(workflow_type=parent_wf_type)

        @child_wf.start()
        async def child_start(ctx: Context) -> Directive:
            return ctx.next.complete("child-done")

        @parent_wf.start()
        async def parent_start(ctx: Context) -> Directive:
            return ctx.next.step(parent_main)

        @parent_wf.step()
        async def parent_main(ctx: Context) -> Directive:
            child_id = str(_ulid.ULID())  # non-deterministic: different on each call
            await ctx.start(child_wf_type, child_id)
            return ctx.next.complete("done")

        conn = await Connection.connect(servers=[nats_url])
        wk = Worker(workflows=[parent_wf, child_wf], connection=conn)
        await wk.start()

    asyncio.run(run())


# ─── Tests ────────────────────────────────────────────────────────────────────


async def test_ctx_start_skips_duplicate_child_on_step_retry(grctl_client: Client) -> None:
    pause_event = multiprocessing.Event()
    parent_wf_type = unique_workflow_type("spec_child_replay_start_parent")
    child_wf_type = unique_workflow_type("spec_child_replay_start_child")

    worker_a = multiprocessing.Process(
        target=_ctx_start_replay_worker, args=(parent_wf_type, child_wf_type, pause_event), daemon=True
    )
    worker_b = multiprocessing.Process(
        target=_ctx_start_replay_worker, args=(parent_wf_type, child_wf_type), daemon=True
    )
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=parent_wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        await history.wait_for_kind(HistoryKind.child_started)
        _terminate(worker_a)
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=60.0)

        assert result == "parent-done"

        all_events = await history.direct_events()
        child_started_events = [e for e in all_events if e.kind == HistoryKind.child_started]
        assert len(child_started_events) == 1, "child must be started exactly once — no duplicate on step retry"
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_send_to_parent_skips_duplicate_event_on_step_retry(grctl_client: Client) -> None:
    pause_event = multiprocessing.Event()
    parent_wf_type = unique_workflow_type("spec_child_replay_send_parent")
    child_wf_type = unique_workflow_type("spec_child_replay_send_child")

    worker_a = multiprocessing.Process(
        target=_send_to_parent_replay_worker, args=(parent_wf_type, child_wf_type, pause_event), daemon=True
    )
    worker_b = multiprocessing.Process(
        target=_send_to_parent_replay_worker, args=(parent_wf_type, child_wf_type), daemon=True
    )
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=parent_wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT
    )

    try:
        parent_history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        child_started_event, _ = await parent_history.wait_for_kind(HistoryKind.child_started)
        child_started = child_started_event.msg
        assert isinstance(child_started, ChildWorkflowStarted)

        child_history = HistoryAccess(
            grctl_client, child_started.wf_id, child_started.run_id, timeout=_HISTORY_TIMEOUT
        )
        await child_history.wait_for_kind(HistoryKind.parent_event_sent)
        _terminate(worker_a)
        worker_b.start()

        result = await asyncio.wait_for(handle.future, timeout=60.0)

        assert result == "parent-done"

        child_events = await child_history.direct_events()
        event_sent_events = [e for e in child_events if e.kind == HistoryKind.parent_event_sent]
        assert len(event_sent_events) == 1, "send_to_parent must fire exactly once — no duplicate on step retry"
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_nondeterminism_raises_when_send_to_parent_event_name_changes(grctl_client: Client) -> None:
    pause_event = multiprocessing.Event()
    parent_wf_type = unique_workflow_type("spec_child_replay_ndet_send_parent")
    child_wf_type = unique_workflow_type("spec_child_replay_ndet_send_child")

    worker_a = multiprocessing.Process(
        target=_ndet_send_event_v1_worker, args=(parent_wf_type, child_wf_type, pause_event), daemon=True
    )
    worker_b = multiprocessing.Process(
        target=_ndet_send_event_v2_worker, args=(parent_wf_type, child_wf_type), daemon=True
    )
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=parent_wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT
    )

    child_handle = None
    try:
        parent_history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        child_started_event, _ = await parent_history.wait_for_kind(HistoryKind.child_started)
        child_started = child_started_event.msg
        assert isinstance(child_started, ChildWorkflowStarted)

        child_history = HistoryAccess(
            grctl_client, child_started.wf_id, child_started.run_id, timeout=_HISTORY_TIMEOUT
        )
        await child_history.wait_for_kind(HistoryKind.parent_event_sent)
        _terminate(worker_a)
        worker_b.start()

        # Nondeterminism happens in the child — await the child's future, not the parent's.
        # Worker A already sent "status_v1" to the parent (completing it), then paused.
        # Worker B replays and tries "status_v2" — the op_id mismatch in history raises NonDeterminismError.
        child_handle = await grctl_client.get_workflow_handle(child_started.wf_id)
        with pytest.raises(WorkflowError, match="NonDeterminism"):
            await asyncio.wait_for(child_handle.future, timeout=60.0)
    finally:
        _terminate(worker_a)
        _terminate(worker_b)


async def test_nondeterministic_child_id_raises_nondeterminism_error(grctl_client: Client) -> None:
    pause_event = multiprocessing.Event()
    parent_wf_type = unique_workflow_type("spec_child_replay_ndet_parent")
    child_wf_type = unique_workflow_type("spec_child_replay_ndet_child")

    worker_a = multiprocessing.Process(
        target=_ndet_child_id_v1_worker, args=(parent_wf_type, child_wf_type, pause_event), daemon=True
    )
    worker_b = multiprocessing.Process(
        target=_ndet_child_id_v2_worker, args=(parent_wf_type, child_wf_type), daemon=True
    )
    worker_a.start()
    await asyncio.sleep(_WORKER_INIT_DELAY)

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=parent_wf_type, id=wf_id, input={}, timeout=_WORKFLOW_TIMEOUT
    )

    try:
        history = HistoryAccess(grctl_client, wf_id, handle.run_info.id, timeout=_HISTORY_TIMEOUT)
        await history.wait_for_kind(HistoryKind.child_started)
        _terminate(worker_a)
        worker_b.start()

        with pytest.raises(WorkflowError, match="NonDeterminism"):
            await asyncio.wait_for(handle.future, timeout=60.0)
    finally:
        _terminate(worker_a)
        _terminate(worker_b)
