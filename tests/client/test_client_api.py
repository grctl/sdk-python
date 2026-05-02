from typing import Any
from unittest.mock import MagicMock

import pytest

from grctl.client.client import Client


@pytest.mark.asyncio
async def test_run_workflow_rejects_old_keyword_names() -> None:
    client = Client(connection=MagicMock())
    kwargs: dict[str, Any] = {
        "workflow_type": "Example",
        "workflow_id": "example-1",
        "workflow_input": {},
        "workflow_timeout": None,
    }

    with pytest.raises(TypeError, match="workflow_type"):
        await client.run_workflow(**kwargs)


@pytest.mark.asyncio
async def test_start_workflow_rejects_old_keyword_names() -> None:
    client = Client(connection=MagicMock())
    kwargs: dict[str, Any] = {
        "workflow_type": "Example",
        "workflow_id": "example-1",
        "workflow_input": {},
        "workflow_timeout": None,
    }

    with pytest.raises(TypeError, match="workflow_type"):
        await client.start_workflow(**kwargs)
