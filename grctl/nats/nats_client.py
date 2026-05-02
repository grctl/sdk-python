import nats
from nats.aio.client import Client


async def get_nats_client(servers: list[str], reconnected_cb: object = None) -> Client:
    """Get a NATS client instance."""
    options: dict = {"servers": servers}
    if reconnected_cb is not None:
        options["reconnected_cb"] = reconnected_cb
    return await nats.connect(**options)
