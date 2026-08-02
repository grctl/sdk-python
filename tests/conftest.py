import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from grctl.logging_config import get_logger
from grctl.models import Directive, DirectiveMessage, RunInfo
from grctl.models.directive import DirectiveKind
from grctl.nats.manifest import NatsManifest

logging.getLogger("nats").setLevel(logging.WARNING)
logger = get_logger(__name__)


@pytest.fixture
def manifest():
    """Load NATS manifest for tests."""
    return NatsManifest.load(yaml_path="grctl/nats/nats_manifest.yaml")


@pytest.fixture
def mock_kv_storage(manifest):
    """Create a mock KV storage that properly handles get/put operations.

    Returns a tuple of (js_mock, kv_storage_dict) where kv_storage_dict
    contains the actual stored data.
    """
    kv_storage: dict[str, bytes] = {}

    async def mock_kv_get(key: str):
        if key in kv_storage:
            entry = AsyncMock()
            entry.value = kv_storage[key]
            return entry
        return None

    async def mock_kv_put(key: str, value: bytes):
        kv_storage[key] = value

    js = AsyncMock()
    js.publish = AsyncMock()

    kv = AsyncMock()
    kv.get = AsyncMock(side_effect=mock_kv_get)
    kv.put = AsyncMock(side_effect=mock_kv_put)
    js.key_value = AsyncMock(return_value=kv)

    return js, kv_storage


def create_directive(  # noqa: PLR0913
    msg: DirectiveMessage,
    kind: DirectiveKind,
    directive_id: str,
    run_id: str,
    wf_id: str,
    wf_type: str = "TestWorkflow",
) -> Directive:
    """Create a directive instance with sensible test defaults."""
    return Directive(
        id=directive_id,
        kind=kind,
        run_info=RunInfo(
            id=run_id,
            wf_id=wf_id,
            wf_type=wf_type,
            created_at=datetime.now(UTC),
        ),
        msg=msg,
        timestamp=datetime.now(UTC),
    )
