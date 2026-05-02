from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from grctl.logging_config import get_logger
from grctl.models import Directive, DirectiveMessage, RunInfo, directive_decoder, history_decoder
from grctl.models.directive import DirectiveKind
from grctl.nats.manifest import NatsManifest
from grctl.nats.publisher import Publisher

logger = get_logger(__name__)


@pytest.fixture
def manifest():
    """Load NATS manifest for tests."""
    return NatsManifest.load(yaml_path="grctl/nats/nats_manifest.yaml")


@pytest.fixture
def mock_connection(manifest):
    """Create a mock connection with publisher for testing.

    Returns a tuple of (connection, published_messages_list) where published_messages_list
    will be populated with (subject, decoded_message) tuples during test execution.
    """
    published = []

    async def mock_publish(subject: str, message: Any) -> None:
        history_subject_prefix = (
            manifest._config.subjects["history"].subject_patterns["publish"].removesuffix(".{wf_id}.{run_id}")
        )
        directive_subject_prefix = (
            manifest._config.subjects["directive"]
            .subject_patterns["publish"]
            .removesuffix(".{wf_type}.{wf_id}.{run_id}")
        )

        decoded_message = None
        try:
            if history_subject_prefix in subject:
                decoded_message = history_decoder(message)
            elif directive_subject_prefix in subject:
                decoded_message = directive_decoder(message)
        except Exception as e:
            logger.warning(f"Failed to decode message for subject {subject}: {e}")
            decoded_message = f"<decode error: {e}>"

        published.append((subject, decoded_message or message))

    nc = AsyncMock()
    nc.publish = AsyncMock(side_effect=mock_publish)

    js = AsyncMock()
    js.publish = AsyncMock(side_effect=mock_publish)

    kv = AsyncMock()
    kv.get = AsyncMock(return_value=None)
    js.key_value = AsyncMock(return_value=kv)

    publisher = Publisher(nc, js, manifest)

    connection = AsyncMock()
    connection.nc = nc
    connection._js = js
    connection.js = js
    connection.publisher = publisher
    connection.manifest = manifest

    return connection, published


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
