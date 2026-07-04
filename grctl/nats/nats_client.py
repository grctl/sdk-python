import nats
from nats.aio.client import Client

from grctl.logging_config import get_logger
from grctl.settings import get_settings

logger = get_logger(__name__)


async def _on_disconnected() -> None:
    logger.warning("NATS connection lost")


async def _on_reconnected() -> None:
    logger.info("NATS reconnected")


async def _on_error(e: Exception) -> None:
    logger.error("NATS error: %s", e)


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
