import asyncio
from contextlib import AbstractContextManager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nats.js.api import ConsumerConfig

from grctl.nats.connection import Connection
from grctl.nats.subscriber import Subscriber
from grctl.worker.errors import RegistrationError
from grctl.worker.worker import Worker
from grctl.workflow.workflow import Workflow


def _make_workflow(workflow_type: str) -> Workflow:
    wf = MagicMock(spec=Workflow)
    wf.workflow_type = workflow_type
    return wf


def _patch_registration() -> AbstractContextManager[AsyncMock]:
    """Patch the registration sync so start() tests focus on subscription wiring."""
    return patch("grctl.worker.worker.register_workflow_types", new_callable=AsyncMock)


def _make_connection() -> AsyncMock:
    connection = AsyncMock(spec=Connection)
    connection.js = AsyncMock()
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


@pytest.mark.asyncio
async def test_wait_until_ready_returns_after_startup() -> None:
    wf = _make_workflow("wf_type_a")
    connection = _make_connection()
    worker = Worker(workflows=[wf], connection=connection)

    mock_subscriber = AsyncMock()

    with patch("grctl.worker.worker.Subscriber", return_value=mock_subscriber), _patch_registration():
        start_task = asyncio.create_task(worker.start())
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
        start_task = asyncio.create_task(worker.start())

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
        start_task = asyncio.create_task(worker.start())
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
        await worker.start()

    # Fail-fast: the worker must not subscribe to task subjects when it cannot register.
    mock_subscriber_cls.assert_not_called()
