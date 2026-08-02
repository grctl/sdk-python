from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from nats.js.api import AckPolicy, DeliverPolicy

from grctl.models import HistoryEvent, HistoryKind, RunStarted, TaskCompleted, history_encoder
from grctl.nats.codec import CodecRegistry
from grctl.nats.history_api import NatsHistoryAPI
from grctl.nats.manifest import NatsManifest


def _make_history(js: AsyncMock, manifest: NatsManifest) -> NatsHistoryAPI:
    nc = MagicMock()
    nc.jetstream = MagicMock(return_value=js)
    return NatsHistoryAPI(nc, manifest, CodecRegistry())


@pytest.mark.asyncio
async def test_fetch_step_history_filters_to_operation_events(manifest) -> None:
    wf_id = "wf-1"
    run_id = "run-1"
    js = AsyncMock()
    subscription = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=subscription)

    timestamp = datetime.now(UTC)

    step_started = HistoryEvent(
        wf_id=wf_id,
        run_id=run_id,
        worker_id="worker-1",
        kind=HistoryKind.run_started,
        msg=RunStarted(),
        timestamp=timestamp,
    )
    task_completed = HistoryEvent(
        wf_id=wf_id,
        run_id=run_id,
        worker_id="worker-1",
        kind=HistoryKind.task_completed,
        msg=TaskCompleted(
            task_id="task-1",
            task_name="fetch_data",
            output={"result": True},
            step_name="step-1",
            duration_ms=5,
        ),
        operation_id="fetch_data:1",
        timestamp=timestamp,
    )

    subscription.fetch = AsyncMock(
        side_effect=[
            [
                AsyncMock(data=history_encoder(step_started)),
                AsyncMock(data=history_encoder(task_completed)),
            ],
            TimeoutError(),
        ]
    )

    history = _make_history(js, manifest)
    events = await history.fetch_step_history(wf_id, run_id, history_seq_id=10)

    assert len(events) == 1
    assert events[0].operation_id == "fetch_data:1"
    assert events[0].kind == HistoryKind.task_completed
    js.pull_subscribe.assert_awaited_once()
    assert js.pull_subscribe.await_args.kwargs["subject"] == manifest.history_subject(wf_id=wf_id, run_id=run_id)  # ty:ignore[unresolved-attribute]
    config = js.pull_subscribe.await_args.kwargs["config"]  # ty:ignore[unresolved-attribute]
    assert config.deliver_policy == DeliverPolicy.BY_START_SEQUENCE
    assert config.opt_start_seq == 10
    assert config.ack_policy == AckPolicy.NONE
    subscription.unsubscribe.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_step_history_returns_empty_when_no_operation_events(manifest) -> None:
    js = AsyncMock()
    subscription = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=subscription)
    run_started = HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.run_started,
        msg=RunStarted(),
    )
    subscription.fetch = AsyncMock(
        side_effect=[
            [AsyncMock(data=history_encoder(run_started))],
            TimeoutError(),
        ]
    )

    history = _make_history(js, manifest)
    events = await history.fetch_step_history("wf-1", "run-1", history_seq_id=20)

    assert events == []
    subscription.unsubscribe.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_step_history_preserves_order(manifest: NatsManifest) -> None:
    js = AsyncMock()
    subscription = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=subscription)
    first = HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.task_completed,
        msg=TaskCompleted(
            task_id="task-a",
            task_name="a",
            output={"result": "A"},
            step_name="step-1",
            duration_ms=1,
        ),
        operation_id="a:1",
    )
    second = HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="worker-1",
        timestamp=datetime.now(UTC),
        kind=HistoryKind.task_completed,
        msg=TaskCompleted(
            task_id="task-b",
            task_name="b",
            output={"result": "B"},
            step_name="step-1",
            duration_ms=1,
        ),
        operation_id="b:1",
    )
    subscription.fetch = AsyncMock(
        side_effect=[
            [
                AsyncMock(data=history_encoder(first)),
                AsyncMock(data=history_encoder(second)),
            ],
            TimeoutError(),
        ]
    )

    history = _make_history(js, manifest)
    events = await history.fetch_step_history("wf-1", "run-1", history_seq_id=30)

    assert [event.operation_id for event in events] == ["a:1", "b:1"]
    subscription.unsubscribe.assert_awaited_once()
