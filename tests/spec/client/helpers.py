import asyncio
import time

from grctl.models import HistoryEvent, HistoryKind

_POLL_INTERVAL_SECONDS = 0.1
_HISTORY_TIMEOUT_SECONDS = 5.0

_RUN_HISTORY_KINDS = {
    HistoryKind.run_started,
    HistoryKind.run_completed,
    HistoryKind.run_failed,
    HistoryKind.run_cancelled,
    HistoryKind.run_timeout,
}


async def wait_for_run_history(
    grctl_client,
    wf_id: str,
    run_id: str,
    expected_kinds: list[HistoryKind],
) -> list[HistoryEvent]:
    deadline = time.monotonic() + _HISTORY_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        events = await grctl_client.get_history(wf_id, run_id=run_id)
        run_events = [event for event in events if event.kind in _RUN_HISTORY_KINDS]
        actual_kinds = [event.kind for event in run_events]
        if actual_kinds == expected_kinds:
            return run_events
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)

    raise AssertionError(f"Timed out waiting for run history {expected_kinds!r} for wf_id={wf_id} run_id={run_id}")
