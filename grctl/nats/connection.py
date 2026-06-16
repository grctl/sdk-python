from nats.aio.client import Client as NATSClient
from nats.client import connect
from nats.jetstream import JetStream
from nats.jetstream import new as new_jetstream
from nats.js.client import JetStreamContext

from grctl.logging_config import get_logger
from grctl.nats.manifest import NatsManifest
from grctl.nats.nats_client import get_nats_client
from grctl.nats.publisher import Publisher
from grctl.settings import get_settings

logger = get_logger(__name__)


class Connection:
    _instance: "Connection | None" = None

    def __init__(
        self, nc: NATSClient, js: JetStreamContext, jetstream: JetStream, manifest: NatsManifest, publisher: Publisher
    ) -> None:
        self._nc = nc
        self._js = js
        self._jetstream = jetstream
        self._manifest = manifest
        self._publisher = publisher

    @classmethod
    async def connect(cls, servers: list[str] | None = None) -> "Connection":
        if cls._instance is not None:
            return cls._instance

        if servers is None:
            servers = get_settings().nats_servers

        try:
            manifest = NatsManifest.load()
            nc = await get_nats_client(servers)
            js = nc.jetstream()
            publisher = Publisher(nc, js, manifest)
            js_client = await connect(servers[0])
            jetstream = new_jetstream(js_client)

            logger.debug("NATS connection established and components initialized")
        except Exception:
            logger.exception("Failed to establish Connection")
            raise

        instance = cls(nc, js, jetstream, manifest, publisher)
        cls._instance = instance
        return instance

    @classmethod
    def reset(cls) -> None:
        cls._instance = None

    @property
    def nc(self) -> NATSClient:
        return self._nc

    @property
    def js(self) -> JetStreamContext:
        return self._js

    @property
    def manifest(self) -> NatsManifest:
        return self._manifest

    @property
    def jetstream(self) -> JetStream:
        return self._jetstream

    @property
    def publisher(self) -> Publisher:
        return self._publisher

    async def close(self) -> None:
        await self._nc.drain()
        logger.debug("Connection closed")
