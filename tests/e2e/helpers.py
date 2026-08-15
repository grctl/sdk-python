import asyncio
import multiprocessing
import time
from collections.abc import Callable
from typing import Any

from nats.jetstream import JetStream

from grctl.models import HistoryEvent, HistoryKind
from grctl.nats.codec import MsgspecCodec
from grctl.nats.history_api import NatsHistoryAPI

_POLL_INTERVAL = 0.2


async def _wait_until_released(pause_event: Any | None) -> None:
    if pause_event is None:
        return
    await asyncio.to_thread(pause_event.wait)


async def _wait_for_history_event(  # noqa: PLR0913
    jetstream: JetStream,
    wf_id: str,
    run_id: str,
    kind: HistoryKind,
    timeout_s: float,
    predicate: Callable[[HistoryEvent], bool] | None = None,
    occurrence: int = 0,
) -> HistoryEvent:
    """Poll the run history stream until the matching event is durable."""
    start = time.monotonic()

    while time.monotonic() - start < timeout_s:
        raw_events = await NatsHistoryAPI(jetstream, MsgspecCodec()).get_run_history(wf_id, run_id)

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
