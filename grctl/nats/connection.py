import logging
from collections.abc import Awaitable, Callable

from nats.client import Client as CoreNATSClient
from nats.jetstream import JetStream
from nats.jetstream import new as new_jetstream

from grctl.logging_config import get_logger
from grctl.models import Command, RunInfo
from grctl.nats.cmd_subscriber import WorkerCmdSubscriber
from grctl.nats.codec import MsgspecCodec
from grctl.nats.directive_api import NatsDirectiveAPI
from grctl.nats.history_api import NatsHistoryAPI
from grctl.nats.history_subscriber import NatsHistoryListenerFactory
from grctl.nats.kv_api import NatsKVApi
from grctl.nats.nats_client import get_nats_client
from grctl.nats.wf_subscriber import DirectiveHandler, Subscriber
from grctl.nats.worker_api import NatsWorkerAPI
from grctl.nats.workflow_api import NatsWorkflowAPI
from grctl.serde import SerializerRegistry
from grctl.settings import get_settings

logger = get_logger(__name__)


class Connection:
    """Owns the NATS transport and exposes the raw collaborators it satisfies.

    Callers (worker/client) get objects back from Connection rather than
    importing grctl.nats.* concretes themselves; those objects satisfy the
    protocols exec/workflow define structurally, without Connection needing
    to import those protocols.
    """

    _instance: "Connection | None" = None

    def __init__(
        self,
        client: CoreNATSClient,
        jetstream: JetStream,
        serializers: SerializerRegistry | None = None,
    ) -> None:
        self._client = client
        self._jetstream = jetstream
        self._codec = MsgspecCodec(serializers)

        self._history = NatsHistoryAPI(self._jetstream, self._codec)
        self._listener_factory = NatsHistoryListenerFactory(self._jetstream)
        self._workflow_api = NatsWorkflowAPI(self._jetstream.client, self._codec)
        self._worker_api = NatsWorkerAPI(self._jetstream.client, self._codec)

    @classmethod
    async def connect(
        cls, servers: list[str] | None = None, serializers: SerializerRegistry | None = None
    ) -> "Connection":
        if cls._instance is not None:
            return cls._instance

        if servers is None:
            servers = get_settings().nats_servers

        try:
            client = await get_nats_client(servers)
            jetstream = new_jetstream(client)

            logger.debug("NATS connection established and components initialized")
        except Exception:
            logger.exception("Failed to establish Connection")
            raise

        instance = cls(client, jetstream, serializers)
        cls._instance = instance
        return instance

    @classmethod
    def reset(cls) -> None:
        cls._instance = None

    @property
    def jetstream(self) -> JetStream:
        return self._jetstream

    @property
    def codec(self) -> MsgspecCodec:
        return self._codec

    async def close(self) -> None:
        await self._client.drain()
        logger.debug("Connection closed")

    @property
    def history_reader(self) -> NatsHistoryAPI:
        return self._history

    @property
    def history_writer(self) -> NatsHistoryAPI:
        return self._history

    @property
    def workflow_api(self) -> NatsWorkflowAPI:
        return self._workflow_api

    @property
    def listener_factory(self) -> NatsHistoryListenerFactory:
        return self._listener_factory

    def build_kv_api(self, run_info: RunInfo) -> NatsKVApi:
        return NatsKVApi(self._jetstream, run_info)

    def build_directive_api(self, run_info: RunInfo) -> NatsDirectiveAPI:
        return NatsDirectiveAPI(self._jetstream, run_info, enc_hook=self._codec.enc_hook)

    def build_worker_cmd_listener(
        self, worker_id: str, handler: Callable[[Command], Awaitable[bool]]
    ) -> WorkerCmdSubscriber:
        return WorkerCmdSubscriber(self._jetstream.client, worker_id, handler)

    def build_exec_job_listener(
        self, wf_types: list[str], directive_handler: DirectiveHandler, logger: logging.Logger
    ) -> Subscriber:
        return Subscriber(self._jetstream, wf_types, directive_handler, logger)

    @property
    def worker_api(self) -> NatsWorkerAPI:
        return self._worker_api
