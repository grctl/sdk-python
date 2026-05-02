from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grctl.nats.connection import Connection
from grctl.nats.manifest import NatsManifest
from grctl.nats.publisher import Publisher


@pytest.fixture(autouse=True)
def reset_connection():
    yield
    Connection.reset()


def make_mock_nc():
    nc = AsyncMock()
    nc.jetstream = MagicMock(return_value=AsyncMock())
    return nc


@pytest.mark.asyncio
async def test_connect_sets_instance():
    mock_nc = make_mock_nc()
    mock_manifest = MagicMock(spec=NatsManifest)

    with (
        patch("grctl.nats.connection.NatsManifest.load", return_value=mock_manifest),
        patch("grctl.nats.connection.get_nats_client", return_value=mock_nc),
    ):
        conn = await Connection.connect()
        conn2 = await Connection.connect()

    assert conn2 is conn


@pytest.mark.asyncio
async def test_connect_failure_does_not_set_instance():
    with (
        patch("grctl.nats.connection.NatsManifest.load", side_effect=RuntimeError("manifest error")),
        pytest.raises(RuntimeError, match="manifest error"),
    ):
        await Connection.connect()

    assert Connection._instance is None


@pytest.mark.asyncio
async def test_reset_clears_instance():
    mock_nc = make_mock_nc()
    mock_manifest = MagicMock(spec=NatsManifest)

    with (
        patch("grctl.nats.connection.NatsManifest.load", return_value=mock_manifest),
        patch("grctl.nats.connection.get_nats_client", return_value=mock_nc),
    ):
        await Connection.connect()

    Connection.reset()

    assert Connection._instance is None


@pytest.mark.asyncio
async def test_properties_return_correct_values():
    mock_nc = make_mock_nc()
    mock_js = mock_nc.jetstream.return_value
    mock_manifest = MagicMock(spec=NatsManifest)

    with (
        patch("grctl.nats.connection.NatsManifest.load", return_value=mock_manifest),
        patch("grctl.nats.connection.get_nats_client", return_value=mock_nc),
    ):
        conn = await Connection.connect()

    assert conn.nc is mock_nc
    assert conn.js is mock_js
    assert conn.manifest is mock_manifest
    assert isinstance(conn.publisher, Publisher)
