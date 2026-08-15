from typing import Protocol

import nats
from nats.aio.client import Client
from nats.client import Client as CoreNATSClient
from nats.client import connect as connect_core
from nats.client.message import Message

from grctl.logging_config import get_logger
from grctl.settings import get_settings

logger = get_logger(__name__)


class CoreRequestClient(Protocol):
    """Core NATS capability needed by request/reply API adapters."""

    async def request(self, subject: str, payload: bytes, *, timeout: float) -> Message: ...  # noqa: ASYNC109


async def _on_disconnected() -> None:
    logger.warning("NATS connection lost")


async def _on_reconnected() -> None:
    logger.info("NATS reconnected")


async def _on_error(e: Exception) -> None:
    logger.error("NATS error: %s", e)


def _on_core_disconnected() -> None:
    logger.warning("NATS connection lost")


def _on_core_reconnected() -> None:
    logger.info("NATS reconnected")


def _on_core_error(error: Exception | str) -> None:
    logger.error("NATS error: %s", error)


async def get_nats_client(servers: list[str], reconnected_cb: object = None) -> Client:
    settings = get_settings()
    options: dict = {
        "servers": servers,
        "connect_timeout": settings.nats_connect_timeout,
        "max_reconnect_attempts": settings.nats_max_reconnect_attempts,
        "reconnect_time_wait": settings.nats_reconnect_time_wait,
        "disconnected_cb": _on_disconnected,
        "reconnected_cb": _on_reconnected,
        "error_cb": _on_error,
    }
    if reconnected_cb is not None:
        options["reconnected_cb"] = reconnected_cb
    return await nats.connect(**options)


async def get_core_nats_client(servers: list[str]) -> CoreNATSClient:
    """Connect the core NATS client used for request/reply and JetStream APIs."""
    if not servers:
        raise ValueError("at least one NATS server is required")

    settings = get_settings()
    client = await connect_core(
        servers[0],
        timeout=settings.nats_connect_timeout,
        reconnect_max_attempts=max(0, settings.nats_max_reconnect_attempts),
        reconnect_time_wait=settings.nats_reconnect_time_wait,
        reconnect_timeout=settings.nats_connect_timeout,
    )
    client.add_disconnected_callback(_on_core_disconnected)
    client.add_reconnected_callback(_on_core_reconnected)
    client.add_error_callback(_on_core_error)
    return client
