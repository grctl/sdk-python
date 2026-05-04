from nats.js.client import JetStreamContext

from grctl.logging_config import get_logger
from grctl.models import RunInfo
from grctl.nats.manifest import NatsManifest

logger = get_logger(__name__)


class KVStore:
    """NATS KV store for workflow state.

    Provides low-level operations for loading and storing workflow data
    in NATS JetStream KV buckets.
    """

    def __init__(self, js: JetStreamContext, manifest: NatsManifest, run: RunInfo) -> None:
        self._js = js
        self._run = run
        self._kv = None
        self._manifest = manifest

    def _make_key(self, key_name: str) -> str:
        return self._manifest.wf_kv_key(
            self._run.wf_id,
            self._run.id,
            key_name,
        )

    async def load(self, key_name: str) -> bytes | None:
        """Load a single key from the store."""
        full_key = self._make_key(key_name)
        stream_name = self._manifest.state_stream_name()
        try:
            entry = await self._js.get_last_msg(stream_name=stream_name, subject=full_key)
            if entry is None or entry.data is None:
                return None

        except Exception as e:
            if "key not found" in str(e).lower():
                return None
            raise
        else:
            return entry.data
