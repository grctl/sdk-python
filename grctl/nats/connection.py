from nats.aio.client import Client as NATSClient
from nats.client import connect
from nats.jetstream import JetStream
from nats.jetstream import new as new_jetstream
from nats.js.client import JetStreamContext

from grctl.logging_config import get_logger
from grctl.nats.codec import CodecRegistry
from grctl.nats.manifest import NatsManifest
from grctl.nats.nats_client import get_nats_client
from grctl.nats.publisher import Publisher
from grctl.settings import get_settings

logger = get_logger(__name__)

# TODO:
# 1. We need to remove singleton pattern. Users should be able to create multiple connections.
# 2. We need to abstract the connection so, users don't need import from NATS layer. We should keep the public API simple.


class Connection:
    _instance: "Connection | None" = None

    def __init__(  # noqa: PLR0913
        self,
        nc: NATSClient,
        js: JetStreamContext,
        jetstream: JetStream,
        manifest: NatsManifest,
        publisher: Publisher,
        codec: CodecRegistry | None = None,
    ) -> None:
        self._nc = nc
        self._js = js
        self._jetstream = jetstream
        self._manifest = manifest
        self._publisher = publisher
        self._codec = codec or CodecRegistry()

    @classmethod
    async def connect(cls, servers: list[str] | None = None, codec: CodecRegistry | None = None) -> "Connection":
        if cls._instance is not None:
            return cls._instance

        if servers is None:
            servers = get_settings().nats_servers

        try:
            manifest = NatsManifest.load()
            nc = await get_nats_client(servers)
            js = nc.jetstream()
            publisher = Publisher(nc, js, manifest)
            settings = get_settings()
            js_client = await connect(
                servers[0],
                reconnect_max_attempts=0,
                reconnect_time_wait=settings.nats_reconnect_time_wait,
                reconnect_timeout=settings.nats_connect_timeout,
            )
            jetstream = new_jetstream(js_client)

            logger.debug("NATS connection established and components initialized")
            codec = codec or CodecRegistry()
        except Exception:
            logger.exception("Failed to establish Connection")
            raise

        instance = cls(nc, js, jetstream, manifest, publisher, codec)
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

    @property
    def codec(self) -> CodecRegistry:
        return self._codec

    async def close(self) -> None:
        await self._nc.drain()
        logger.debug("Connection closed")
