"""Shared history polling helper for spec tests."""

import asyncio
import time

from grctl.client import Client
from grctl.models import HistoryEvent, HistoryKind, StepStarted
from grctl.nats.connection import Connection as NatsConnection

_POLL_INTERVAL = 0.1
_DEFAULT_TIMEOUT = 10.0

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
        """Return the durable events for this run through the production reader."""
        connection = self._client._connection
        if not isinstance(connection, NatsConnection):
            raise TypeError("HistoryAccess requires a NATS-backed client")
        return await connection.history_reader.get_run_history(self._wf_id, self._run_id)

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

    async def wait_for_step_started(self, step_name: str) -> HistoryEvent:
        """Poll until the named step has been picked up by a worker."""
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            for event in await self.direct_events():
                if isinstance(event.msg, StepStarted) and event.msg.step_name == step_name:
                    return event
            await asyncio.sleep(_POLL_INTERVAL)
        raise AssertionError(
            f"Timed out waiting for step {step_name!r} to start — wf_id={self._wf_id} run_id={self._run_id}"
        )

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
