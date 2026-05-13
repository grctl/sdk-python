from datetime import timedelta

import pytest
import ulid

from grctl.models.errors import WorkflowError
from grctl.worker import Context
from grctl.workflow import Directive, Workflow


def _unique_wf_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


async def test_run_workflow_returns_result(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_run_result"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete("ok")

    await worker([wf])

    result = await grctl_client.run_workflow(
        type=wf.workflow_type,
        id=str(ulid.ULID()),
        input={},
        timeout=timedelta(seconds=30),
    )

    assert result == "ok"


async def test_run_workflow_raises_workflow_error_on_failure(worker, grctl_client) -> None:
    wf = Workflow(workflow_type=_unique_wf_type("spec_client_run_failure"))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        raise ValueError("workflow exploded")

    await worker([wf])

    with pytest.raises(WorkflowError, match="ValueError: workflow exploded"):
        await grctl_client.run_workflow(
            type=wf.workflow_type,
            id=str(ulid.ULID()),
            input={},
            timeout=timedelta(seconds=30),
        )
