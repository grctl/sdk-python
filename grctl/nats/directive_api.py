from collections.abc import Callable
from typing import Any

from nats.jetstream import JetStream

from grctl.models import Directive, RunInfo, directive_encoder
from grctl.nats.manifest import manifest


class NatsDirectiveAPI:
    """NATS impl of the DirectiveAPI outbound port for one run.

    Publishes step directives onto the run's directive subject.
    """

    def __init__(
        self,
        jetstream: JetStream,
        run_info: RunInfo,
        enc_hook: Callable[[Any], Any] | None = None,
    ) -> None:
        self._jetstream = jetstream
        self._run_info = run_info
        self._enc_hook = enc_hook

    async def send(self, directive: Directive) -> None:
        subject = manifest.directive_subject(
            wf_type=self._run_info.wf_type,
            wf_id=self._run_info.wf_id,
            run_id=self._run_info.id,
        )
        data = directive_encoder(directive, enc_hook=self._enc_hook)
        await self._jetstream.publish(subject, data)
