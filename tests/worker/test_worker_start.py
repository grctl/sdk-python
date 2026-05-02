from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nats.js.api import ConsumerConfig

from grctl.nats.connection import Connection
from grctl.nats.subscriber import Subscriber
from grctl.worker.worker import Worker
from grctl.workflow.workflow import Workflow


def _make_workflow(workflow_type: str) -> Workflow:
    wf = MagicMock(spec=Workflow)
    wf.workflow_type = workflow_type
    return wf


def _make_connection() -> AsyncMock:
    connection = AsyncMock(spec=Connection)
    connection.js = AsyncMock()
    connection.manifest = MagicMock()
    connection.publisher = AsyncMock()
    return connection


@pytest.mark.asyncio
async def test_subscribe_called_with_wf_types() -> None:
    wf = _make_workflow("wf_type_a")
    connection = _make_connection()
    worker = Worker(workflows=[wf], connection=connection)

    mock_subscriber = AsyncMock(spec=Subscriber)

    with patch("grctl.worker.worker.Subscriber", return_value=mock_subscriber) as mock_subscriber_cls:
        worker._stop_event.set()
        await worker.start()

    mock_subscriber_cls.assert_called_once()
    call_kwargs = mock_subscriber_cls.call_args.kwargs
    assert call_kwargs["wf_types"] == ["wf_type_a"]
    mock_subscriber.start.assert_called_once()


@pytest.mark.asyncio
async def test_subscriber_uses_configured_worker_ack_wait() -> None:
    connection = _make_connection()
    connection.manifest.worker_task_filter_subject.return_value = "grctl_worker_task.wf_type_a.>"
    connection.manifest.worker_task_queue_group.return_value = "grctl_worker_wf_type_a"
    connection.js.subscribe = AsyncMock()

    with patch("grctl.nats.subscriber.get_settings") as mock_get_settings:
        mock_get_settings.return_value.nats_worker_ack_wait = 7.5
        subscriber = Subscriber(
            js=connection.js,
            manifest=connection.manifest,
            wf_types=["wf_type_a"],
            run_manager=AsyncMock(),
        )

        await subscriber.start()

    subscribe_kwargs = connection.js.subscribe.await_args.kwargs  # ty:ignore[unresolved-attribute]
    assert isinstance(subscribe_kwargs["config"], ConsumerConfig)
    assert subscribe_kwargs["config"].ack_wait == 7.5
