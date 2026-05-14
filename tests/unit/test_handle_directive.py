import asyncio
import logging

from grctl.logging_config import get_logger, setup_logging
from grctl.models import Directive, Start, Step
from grctl.models.directive import DirectiveKind
from grctl.worker.context import Context
from grctl.worker.run_manager import RunManager
from grctl.worker.task import task
from grctl.workflow import Workflow
from tests.conftest import create_directive

setup_logging(level=logging.DEBUG)
logger = get_logger(__name__)


hello_wf = Workflow(workflow_type="Hello")


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


@hello_wf.step()
async def process_greeting(ctx: Context) -> Directive:
    logger.info("Processing greeting step")
    greeting = await call_greeting_api("Step User")
    return ctx.next.complete(greeting)


async def test_start(mock_connection):
    """Test that the directive handling works as expected with KV updates."""
    connection, published = mock_connection

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
        wf_type="Hello",
        directive_id="dir-start-1",
        run_id="run-start-1",
        wf_id="wf-start-1",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    # Find the directive message
    directive_msg = next((msg for _, msg in published if isinstance(msg, Directive)), None)
    assert directive_msg is not None, "Expected to find a Directive message"

    # Workers always publish step_result directives back to the server
    assert directive_msg.kind == "step_result", f"Expected kind='step_result', got {directive_msg.kind}"

    # Verify the next step is complete
    from grctl.models.directive import StepResult  # noqa: PLC0415

    assert isinstance(directive_msg.msg, StepResult)
    assert directive_msg.msg.next_msg_kind == "complete", (
        f"Expected next_msg_kind='complete', got {directive_msg.msg.next_msg_kind}"
    )

    # Verify kv_updates were included in the step result
    assert directive_msg.msg.kv_updates, "Expected kv_updates to be non-empty"


async def test_step(mock_connection):
    """Test that Step directive handling works as expected."""
    connection, published = mock_connection

    manager = RunManager(
        worker_name="test-worker",
        worker_id="worker-1",
        workflows=[hello_wf],
        connection=connection,
    )

    directive = create_directive(
        kind=DirectiveKind.step,
        msg=Step(step_name="process_greeting"),
        wf_type="Hello",
        directive_id="dir-step-1",
        run_id="run-step-1",
        wf_id="wf-step-1",
    )

    await manager.handle_next_directive(directive)
    await manager.shutdown()

    # Find the directive message
    directive_msg = next((msg for _, msg in published if isinstance(msg, Directive)), None)
    assert directive_msg is not None, "Expected to find a Directive message"

    # Workers always publish step_result directives back to the server
    assert directive_msg.kind == "step_result", f"Expected kind='step_result', got {directive_msg.kind}"

    from grctl.models.directive import StepResult  # noqa: PLC0415

    assert isinstance(directive_msg.msg, StepResult)
    assert directive_msg.msg.next_msg_kind == "complete", (
        f"Expected next_msg_kind='complete', got {directive_msg.msg.next_msg_kind}"
    )
