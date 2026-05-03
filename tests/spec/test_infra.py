"""Infrastructure smoke test: verifies grctld starts and a basic workflow completes."""

from datetime import timedelta

import ulid

from tests.spec.workflows import simple_wf


async def test_grctld_starts_and_workflow_completes_end_to_end(grctl_client, worker):
    await worker([simple_wf])

    result = await grctl_client.run_workflow(
        type=simple_wf.workflow_type,
        id=str(ulid.ULID()),
        input={"value": "hello"},
        timeout=timedelta(seconds=15),
    )

    assert result == "hello"
