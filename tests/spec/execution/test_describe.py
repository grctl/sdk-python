import asyncio
from datetime import timedelta

import pytest
import ulid

from grctl.models import HistoryKind
from grctl.models.errors import WorkflowNotFoundError
from tests.spec.history import HistoryAccess
from tests.spec.workflows import make_waiting_event_workflow


async def test_describe_returns_run_info_for_started_workflow(worker, grctl_client) -> None:
    wf = make_waiting_event_workflow(prefix="spec_execution_describe")
    await worker([wf])

    wf_id = str(ulid.ULID())
    handle = await grctl_client.start_workflow(
        type=wf.workflow_type,
        id=wf_id,
        input={},
        timeout=timedelta(seconds=30),
    )
    await HistoryAccess(grctl_client, wf_id, handle.run_info.id).wait_for_run([HistoryKind.run_started])

    try:
        run_info = await grctl_client.describe(wf_id)

        assert run_info.id == handle.run_info.id
        assert run_info.wf_id == wf_id
        assert run_info.wf_type == wf.workflow_type

        await handle.send("finish", {"result": "ok"})
        assert await asyncio.wait_for(handle.future, timeout=30) == "ok"
    finally:
        await handle.future.stop()


async def test_describe_raises_not_found_for_unknown_wf_id(grctl_client) -> None:
    with pytest.raises(WorkflowNotFoundError):
        await grctl_client.describe(str(ulid.ULID()))
