from nats.aio.client import Client as NATSClient

from grctl.models import Command, RunInfo, command_encoder
from grctl.nats.codec import CodecRegistry
from grctl.nats.manifest import NatsManifest

_REQUEST_TIMEOUT_SECONDS = 5.0


class NatsCommandSender:
    """Delivers a Command to a run over NATS request/reply and returns the raw response."""

    def __init__(self, nc: NATSClient, manifest: NatsManifest, codec: CodecRegistry) -> None:
        self._nc = nc
        self._manifest = manifest
        self._codec = codec

    async def send(self, run: RunInfo, cmd: Command) -> bytes:
        subject = self._manifest.api_subject(wf_id=run.wf_id)
        data = command_encoder(cmd, enc_hook=self._codec.enc_hook)
        msg = await self._nc.request(subject, data, timeout=_REQUEST_TIMEOUT_SECONDS)
        return msg.data
