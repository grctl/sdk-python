from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_completing_workflow


async def test_get_history_returns_ordered_run_events(worker, grctl_client) -> None:
    wf = make_completing_workflow(prefix="spec_execution_history_ordered")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )

    assert await handle.future == "ok"

    run_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run(
        [HistoryKind.run_started, HistoryKind.run_completed]
    )

    assert [event.kind for event in run_events] == [HistoryKind.run_started, HistoryKind.run_completed]


async def test_get_history_with_explicit_run_id_skips_describe(
    worker,
    grctl_client,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wf = make_completing_workflow(prefix="spec_execution_history_explicit_run")
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

    run_events = await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run(
        [HistoryKind.run_started, HistoryKind.run_completed]
    )

    assert [event.kind for event in run_events] == [HistoryKind.run_started, HistoryKind.run_completed]


async def test_get_history_without_run_id_resolves_latest_run(worker, grctl_client) -> None:
    wf = make_completing_workflow(result="complete", prefix="spec_execution_history_latest")
    await worker([wf])

    wf_id = str(ulid.ULID())
    first_handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    assert await first_handle.future == "complete"
    await HistoryAccess(grctl_client, wf_id, first_handle.run_info.id).wait_for_run(
        [HistoryKind.run_started, HistoryKind.run_completed]
    )

    second_handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    assert await second_handle.future == "complete"
    await HistoryAccess(grctl_client, wf_id, second_handle.run_info.id).wait_for_run(
        [HistoryKind.run_started, HistoryKind.run_completed]
    )

    events = await grctl_client.get_history(wf_id)

    assert events
    assert {event.run_id for event in events} == {second_handle.run_info.id}
