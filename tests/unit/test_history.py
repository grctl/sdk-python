import asyncio
import logging

from grctl.logging_config import get_logger, setup_logging
from grctl.models import (
    Directive,
    DirectiveKind,
    Start,
    StepStarted,
    TaskCompleted,
    TaskStarted,
)
from grctl.worker.context import Context
from grctl.worker.run_manager import RunManager
from grctl.worker.task import task
from grctl.workflow import Workflow
from tests.conftest import create_directive

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


async def test_history_events_published(mock_connection):
    """Test that all expected history events are published during workflow execution."""
    connection, published = mock_connection

    hello_wf = Workflow(workflow_type="HistoryTest")

    @task
    async def call_greeting_api(name: str) -> str:
        logger.info(f"Calling external Greeting API for name: {name}")
        await asyncio.sleep(0.1)  # Simulate network delay
        return f"Hello, {name}!"

    @hello_wf.start()
    async def start(ctx: Context, name: str) -> Directive:
        logger.info(f"Initialized workflow for: {name}")
        ctx.store.put("name", name)
        greeting = await call_greeting_api(name)
        message = f"{greeting}"
        ctx.store.put("message", message)
        return ctx.next.complete(message)

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[hello_wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.start,
        msg=Start(
            input={"name": "Test User"},
        ),
        directive_id="dir-history-1",
        run_id="run-history-1",
        wf_id="wf-history-1",
        wf_type="HistoryTest",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    # Filter history events
    history_events = [msg for subject, msg in published if "grctl_history" in subject and not isinstance(msg, str)]

    logger.info(f"Published {len(history_events)} history events")
    for event in history_events:
        logger.info(f"  - {type(event).__name__}: {event.kind}")

    # Verify we have the expected number of history events
    # Expected: StepStarted, TaskStarted, TaskCompleted
    # Note: RunStarted and StepCompleted are published by the server, not the worker
    assert len(history_events) == 3, f"Expected 3 history events, got {len(history_events)}"

    # Verify event types in order
    assert isinstance(history_events[0].msg, StepStarted), (
        f"Expected StepStarted, got {type(history_events[0].msg).__name__}"
    )
    assert isinstance(history_events[1].msg, TaskStarted), (
        f"Expected TaskStarted, got {type(history_events[1].msg).__name__}"
    )
    assert isinstance(history_events[2].msg, TaskCompleted), (
        f"Expected TaskCompleted, got {type(history_events[2].msg).__name__}"
    )

    # Verify StepStarted event
    step_started = history_events[0].msg
    assert step_started.step_name == "start"

    # Verify TaskStarted event
    task_started = history_events[1].msg
    assert task_started.task_name == "call_greeting_api"
    assert task_started.step_name == "start"
    assert task_started.args == {"name": "Test User"}

    # Verify TaskCompleted event
    task_completed = history_events[2].msg
    assert task_completed.task_name == "call_greeting_api"
    assert task_completed.step_name == "start"
    assert task_completed.output == {"result": "Hello, Test User!"}
    assert task_completed.duration_ms > 0

    logger.info("All history events verified successfully")
