"""Shared history polling helper for spec tests."""

import asyncio
import time

from nats.js.errors import NotFoundError

from grctl.client import Client
from grctl.models import HistoryEvent, HistoryKind, history_decoder

_POLL_INTERVAL = 0.1
_DEFAULT_TIMEOUT = 5.0

_RUN_KINDS = frozenset(
    {
        HistoryKind.run_started,
        HistoryKind.run_completed,
        HistoryKind.run_failed,
        HistoryKind.run_cancelled,
        HistoryKind.run_timeout,
    }
)

_STEP_KINDS = frozenset(
    {
        HistoryKind.step_started,
        HistoryKind.step_completed,
        HistoryKind.step_failed,
        HistoryKind.step_cancelled,
        HistoryKind.step_timeout,
    }
)

_TASK_KINDS = frozenset(
    {
        HistoryKind.task_started,
        HistoryKind.task_completed,
        HistoryKind.task_attempt_failed,
        HistoryKind.task_failed,
        HistoryKind.task_cancelled,
    }
)


class HistoryAccess:
    """Polls workflow history for expected events.

    Args:
        grctl_client: Connected grctl Client instance.
        wf_id: Workflow ID to poll.
        run_id: Run ID to poll.
        timeout: Seconds before raising AssertionError (default 5.0).

    """

    def __init__(self, grctl_client: Client, wf_id: str, run_id: str, timeout: float = _DEFAULT_TIMEOUT) -> None:
        self._client = grctl_client
        self._wf_id = wf_id
        self._run_id = run_id
        self._timeout = timeout

    async def events(self) -> list[HistoryEvent]:
        """Return all history events for this run."""
        return await self._client.get_history(self._wf_id, run_id=self._run_id)

    async def direct_events(self) -> list[HistoryEvent]:
        """Return history events without creating a pull consumer."""
        connection = self._client._connection
        manifest = connection.manifest
        subject = manifest.history_subject(wf_id=self._wf_id, run_id=self._run_id)
        stream = manifest.history_stream_name()
        manager = connection.js._jsm

        try:
            last = await manager.get_last_msg(stream, subject=subject, direct=True)
        except NotFoundError:
            return []

        events: list[HistoryEvent] = []
        next_seq = 1
        while next_seq <= last.seq:  # ty:ignore[unsupported-operator]
            try:
                raw_msg = await manager.get_msg(stream, seq=next_seq, subject=subject, next=True, direct=True)
            except NotFoundError:
                break
            if raw_msg.data:
                events.append(history_decoder(raw_msg.data))
            next_seq = raw_msg.seq + 1  # ty:ignore[unsupported-operator]

        return events

    async def wait_for_kind(self, kind: HistoryKind) -> tuple[HistoryEvent, list[HistoryEvent]]:
        """Poll until an event of the given kind appears, then return it and all events from that fetch."""
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            events = await self.direct_events()
            for event in events:
                if event.kind == kind:
                    return event, events
            await asyncio.sleep(_POLL_INTERVAL)
        raise AssertionError(f"Timed out waiting for {kind!r} — wf_id={self._wf_id} run_id={self._run_id}")

    async def wait_for_run(self, expected_kinds: list[HistoryKind]) -> list[HistoryEvent]:
        """Poll until run events match the expected sequence, then return them."""
        return await self._wait_for_filtered(expected_kinds, _RUN_KINDS, "run")

    async def wait_for_step(self, expected_kinds: list[HistoryKind]) -> list[HistoryEvent]:
        """Poll until step events match the expected sequence, then return them."""
        return await self._wait_for_filtered(expected_kinds, _STEP_KINDS, "step")

    async def wait_for_task(self, expected_kinds: list[HistoryKind]) -> list[HistoryEvent]:
        """Poll until task events match the expected sequence, then return them."""
        return await self._wait_for_filtered(expected_kinds, _TASK_KINDS, "task")

    async def _wait_for_filtered(
        self,
        expected_kinds: list[HistoryKind],
        kind_filter: frozenset[HistoryKind],
        label: str,
    ) -> list[HistoryEvent]:
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            events = [e for e in await self.direct_events() if e.kind in kind_filter]
            if [e.kind for e in events] == expected_kinds:
                return events
            await asyncio.sleep(_POLL_INTERVAL)
        raise AssertionError(
            f"Timed out waiting for {label} history {expected_kinds!r} — wf_id={self._wf_id} run_id={self._run_id}"
        )
