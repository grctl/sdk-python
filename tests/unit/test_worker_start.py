import asyncio
import logging
from contextlib import AbstractContextManager
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nats.jetstream.consumer import ConsumerConfig

from grctl.nats.connection import Connection
from grctl.nats.wf_subscriber import Subscriber
from grctl.worker.errors import RegistrationError
from grctl.worker.worker import Worker
from grctl.workflow.workflow import Workflow


def _make_workflow(workflow_type: str) -> Workflow:
    wf = MagicMock(spec=Workflow)
    wf.workflow_type = workflow_type
    wf.start_handler = None
    wf.start_step_name = workflow_type
    wf.step_names = []
    wf.event_names = []
    wf.query_names = []
    return wf


def _patch_registration() -> AbstractContextManager[AsyncMock]:
    """Patch the registration sync so start() tests focus on subscription wiring."""
    return patch("grctl.worker.worker.register_workflow_types", new_callable=AsyncMock)


def _make_connection() -> AsyncMock:
    connection = AsyncMock(spec=Connection)
    connection.js = AsyncMock()
    connection.jetstream = AsyncMock()
    connection.manifest = MagicMock()
    connection.publisher = AsyncMock()
    connection.nc = AsyncMock()
    return connection


@pytest.mark.asyncio
async def test_subscribe_called_with_wf_types() -> None:
    wf = _make_workflow("wf_type_a")
    connection = _make_connection()
    worker = Worker(workflows=[wf], connection=connection)

    mock_subscriber = AsyncMock()

    with (
        patch("grctl.worker.worker.Subscriber", return_value=mock_subscriber) as mock_subscriber_cls,
        _patch_registration(),
    ):
        worker._stop_event.set()
        await worker.run()

    mock_subscriber_cls.assert_called_once()
    call_kwargs = mock_subscriber_cls.call_args.kwargs
    assert call_kwargs["wf_types"] == ["wf_type_a"]
    mock_subscriber.start.assert_called_once()


@pytest.mark.asyncio
async def test_subscriber_uses_configured_worker_ack_wait() -> None:
    manifest = MagicMock()
    manifest.worker_task_filter_subject.return_value = "grctl_worker_task.wf_type_a.>"
    manifest.worker_task_queue_group.return_value = "grctl_worker_wf_type_a"
    manifest.state_stream_name.return_value = "grctl_state"

    consumer = AsyncMock()
    consumer.messages = AsyncMock(return_value=AsyncMock(__aiter__=lambda s: iter([])))
    stream = AsyncMock()
    stream.create_or_update_consumer = AsyncMock(return_value=consumer)
    js = AsyncMock()
    js.get_stream = AsyncMock(return_value=stream)

    with patch("grctl.nats.wf_subscriber.get_settings") as mock_get_settings:
        mock_get_settings.return_value.nats_worker_ack_wait = 7.5
        mock_get_settings.return_value.progress_ack_interval_seconds = 5
        subscriber = Subscriber(
            js=js,
            manifest=manifest,
            wf_types=["wf_type_a"],
            run_manager=AsyncMock(),
            logger=logging.getLogger(__name__),
        )

        await subscriber.start()

    assert stream.create_or_update_consumer.await_args is not None
    config = stream.create_or_update_consumer.await_args.args[0]
    assert isinstance(config, ConsumerConfig)
    assert config.ack_wait == timedelta(seconds=7.5)


@pytest.mark.asyncio
async def test_wait_until_ready_returns_after_startup() -> None:
    wf = _make_workflow("wf_type_a")
    connection = _make_connection()
    worker = Worker(workflows=[wf], connection=connection)

    mock_subscriber = AsyncMock()

    with patch("grctl.worker.worker.Subscriber", return_value=mock_subscriber), _patch_registration():
        start_task = asyncio.create_task(worker.run())
        await worker.wait_until_ready()
        await worker.stop()
        await start_task


@pytest.mark.asyncio
async def test_wait_until_ready_propagates_startup_failure() -> None:
    wf = _make_workflow("wf_type_a")
    connection = _make_connection()
    worker = Worker(workflows=[wf], connection=connection)
    startup_error = RuntimeError("boom")

    mock_subscriber = AsyncMock()
    mock_subscriber.start.side_effect = startup_error

    with patch("grctl.worker.worker.Subscriber", return_value=mock_subscriber), _patch_registration():
        start_task = asyncio.create_task(worker.run())

        with pytest.raises(RuntimeError, match="boom"):
            await worker.wait_until_ready()

        with pytest.raises(RuntimeError, match="boom"):
            await start_task


@pytest.mark.asyncio
async def test_worker_cmd_subscriber_lifecycle() -> None:
    wf = _make_workflow("wf_type_a")
    connection = _make_connection()

    worker = Worker(workflows=[wf], connection=connection)

    mock_subscriber = AsyncMock()
    mock_cmd_subscriber = AsyncMock()

    with (
        patch("grctl.worker.worker.Subscriber", return_value=mock_subscriber),
        patch("grctl.worker.worker.WorkerCmdSubscriber", return_value=mock_cmd_subscriber),
        _patch_registration(),
    ):
        start_task = asyncio.create_task(worker.run())
        await worker.wait_until_ready()

        mock_cmd_subscriber.start.assert_called_once()

        await worker.stop()
        await start_task

    mock_cmd_subscriber.stop.assert_called_once()


@pytest.mark.asyncio
async def test_registration_failure_aborts_before_subscribing() -> None:
    wf = _make_workflow("wf_type_a")
    connection = _make_connection()
    worker = Worker(workflows=[wf], connection=connection)

    with (
        patch("grctl.worker.worker.Subscriber") as mock_subscriber_cls,
        patch(
            "grctl.worker.worker.register_workflow_types",
            new_callable=AsyncMock,
            side_effect=RegistrationError("registration exhausted"),
        ),
        pytest.raises(RegistrationError, match="registration exhausted"),
    ):
        await worker.run()

    # Fail-fast: the worker must not subscribe to task subjects when it cannot register.
    mock_subscriber_cls.assert_not_called()
