from collections.abc import Callable
from typing import Any

from nats.aio.client import Client as NATSClient
from nats.js.client import JetStreamContext

from grctl.models import Command, Directive, HistoryEvent, RunInfo, command_encoder, directive_encoder, history_encoder
from grctl.nats.manifest import NatsManifest


class Publisher:
    def __init__(self, nc: NATSClient, js: JetStreamContext, manifest: NatsManifest) -> None:
        self._nc = nc
        self._js = js
        self._manifest = manifest

    async def publish_history(
        self, run_info: RunInfo, event: HistoryEvent, enc_hook: Callable[[Any], Any] | None = None
    ) -> None:
        subject = self._manifest.history_subject(wf_id=run_info.wf_id, run_id=run_info.id)
        data = history_encoder(event, enc_hook=enc_hook)
        await self._js.publish(subject, data)

    async def publish_next_directive(
        self,
        run: RunInfo,
        directive: Directive,
        enc_hook: Callable[[Any], Any] | None = None,
    ) -> None:
        subject = self._manifest.directive_subject(
            wf_type=run.wf_type,
            wf_id=run.wf_id,
            run_id=run.id,
        )
        data = directive_encoder(directive, enc_hook=enc_hook)
        await self._js.publish(subject, data)

    async def publish_cmd(
        self,
        run: RunInfo,
        cmd: Command,
    ) -> bytes:
        subject = self._manifest.api_subject(wf_id=run.wf_id)
        data = command_encoder(cmd)
        msg = await self._nc.request(subject, data)
        return msg.data
