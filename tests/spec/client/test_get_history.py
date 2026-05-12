from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.worker import Context
from grctl.workflow import Directive, Workflow
from tests.spec.client.helpers import wait_for_run_history


def _unique_wf_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


def _build_completing_workflow(prefix: str, result: str = "ok") -> Workflow:
    wf = Workflow(workflow_type=_unique_wf_type(prefix))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(result)

    return wf


async def test_get_history_returns_ordered_run_events(worker, grctl_client) -> None:
    wf = _build_completing_workflow("spec_client_history_ordered")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    assert await handle.future == "ok"

    run_events = await wait_for_run_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [HistoryKind.run_started, HistoryKind.run_completed],
    )

    assert [event.kind for event in run_events] == [HistoryKind.run_started, HistoryKind.run_completed]


async def test_get_history_with_explicit_run_id_skips_describe(
    worker,
    grctl_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wf = _build_completing_workflow("spec_client_history_explicit_run")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    assert await handle.future == "ok"

    async def fail_describe(wf_id: str):
        raise AssertionError(f"describe should not be called for explicit run_id: {wf_id}")

    monkeypatch.setattr(grctl_client, "describe", fail_describe)

    run_events = await wait_for_run_history(
        grctl_client,
        wf_id,
        handle.run_info.id,
        [HistoryKind.run_started, HistoryKind.run_completed],
    )

    assert [event.kind for event in run_events] == [HistoryKind.run_started, HistoryKind.run_completed]


async def test_get_history_without_run_id_resolves_latest_run(worker, grctl_client) -> None:
    wf = _build_completing_workflow("spec_client_history_latest", result="complete")
    await worker([wf])

    wf_id = str(ulid.ULID())
    first_handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    assert await first_handle.future == "complete"
    await wait_for_run_history(
        grctl_client,
        wf_id,
        first_handle.run_info.id,
        [HistoryKind.run_started, HistoryKind.run_completed],
    )

    second_handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    assert await second_handle.future == "complete"
    await wait_for_run_history(
        grctl_client,
        wf_id,
        second_handle.run_info.id,
        [HistoryKind.run_started, HistoryKind.run_completed],
    )

    events = await grctl_client.get_history(wf_id)

    assert events
    assert {event.run_id for event in events} == {second_handle.run_info.id}
