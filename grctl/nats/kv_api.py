from typing import Any

import msgspec
from nats.js.client import JetStreamContext

from grctl.logging_config import get_logger
from grctl.models import RunInfo
from grctl.nats.manifest import manifest

logger = get_logger(__name__)


class NatsKVApi:
    """NATS impl of the KVApi read port for one run's workflow state.

    Loads durable values from the state stream in JetStream. Writes are not
    exposed here — they flow through the server as step directives.
    """

    def __init__(self, js: JetStreamContext, run: RunInfo) -> None:
        self._js = js
        self._run = run
        self._kv = None

    def _make_key(self, key_name: str) -> str:
        return manifest.wf_kv_key(
            self._run.wf_id,
            self._run.id,
            key_name,
        )

    async def load(self, key: str, ty: type | None = None) -> Any:  # noqa: ARG002
        """Load and decode a single key from the store.

        `ty` is accepted to satisfy the KVApi protocol but unused — KVManager
        falls back to its Caster when the decoded value isn't already of type `ty`.
        """
        full_key = self._make_key(key)
        stream_name = manifest.state_stream_name()
        try:
            entry = await self._js.get_last_msg(stream_name=stream_name, subject=full_key)
            if entry is None or entry.data is None:
                return None

        except Exception as e:
            msg = str(e).lower()
            if "key not found" in msg or "no message found" in msg:
                return None
            raise
        else:
            return msgspec.msgpack.decode(entry.data)
