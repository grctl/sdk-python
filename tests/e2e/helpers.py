import asyncio
import multiprocessing
import time
from collections.abc import Callable
from typing import Any

from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.client import JetStreamContext
from nats.js.errors import FetchTimeoutError

from grctl.models import HistoryEvent, HistoryKind, history_decoder
from grctl.nats.manifest import NatsManifest

_POLL_INTERVAL = 0.2


async def _wait_until_released(pause_event: Any | None) -> None:
    if pause_event is None:
        return
    await asyncio.to_thread(pause_event.wait)


async def _wait_for_history_event(  # noqa: PLR0913
    js: JetStreamContext,
    manifest: NatsManifest,
    wf_id: str,
    run_id: str,
    kind: HistoryKind,
    timeout_s: float,
    predicate: Callable[[HistoryEvent], bool] | None = None,
    occurrence: int = 0,
) -> HistoryEvent:
    """Poll the run history stream until the matching event is durable."""
    history_subject = manifest.history_subject(wf_id=wf_id, run_id=run_id)
    history_stream = manifest.history_stream_name()
    start = time.monotonic()

    while time.monotonic() - start < timeout_s:
        subscription = await js.pull_subscribe(
            subject=history_subject,
            stream=history_stream,
            config=ConsumerConfig(
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.NONE,
                inactive_threshold=1.0,
            ),
        )
        try:
            raw_events: list[HistoryEvent] = []
            try:
                while True:
                    messages = await subscription.fetch(batch=256, timeout=0.25)
                    raw_events.extend(history_decoder(msg.data) for msg in messages if msg.data)
            except (TimeoutError, FetchTimeoutError):
                pass
        finally:
            await subscription.unsubscribe()

        matches = [
            event
            for event in raw_events
            if event.kind == kind and (predicate(event) if predicate is not None else True)
        ]
        if len(matches) > occurrence:
            return matches[occurrence]

        await asyncio.sleep(_POLL_INTERVAL)

    raise TimeoutError(
        f"History event {kind!s} occurrence={occurrence} for wf_id={wf_id!r} run_id={run_id!r} "
        f"not found within {timeout_s}s"
    )


def _terminate_process(process: multiprocessing.Process) -> None:
    if process.is_alive():
        process.terminate()
        process.join(timeout=5.0)
        if process.is_alive():
            process.kill()
            process.join(timeout=1.0)
