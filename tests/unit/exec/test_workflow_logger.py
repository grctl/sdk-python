"""What a step logs through ctx.logger, and what it deliberately does not log again."""

import logging

import pytest

from grctl.exec.context import Context
from grctl.exec.execution import Execution, ExecutionDeps
from grctl.exec.kv_manager import KVManager
from grctl.logging_config import get_logger
from grctl.models import Directive, DirectiveKind, HistoryEvent, Step
from grctl.models.handler import HandlerSpec, RegisteredStep
from grctl.models.worker import WorkerInfo
from grctl.nats.codec import MsgspecCodec
from grctl.workflow.handle import WorkflowHandleFactory
from grctl.workflow.workflow import StepInfo
from tests.unit.exec.fakes import (
    DEFAULT_RUN_INFO,
    DEFAULT_WORKER_ID,
    FakeAppender,
    FakeHistoryListenerFactory,
    FakeKVApi,
    FakeWorkflowAPI,
    make_context,
)


@pytest.fixture(autouse=True)
def capture_all_levels(caplog):
    with caplog.at_level(logging.DEBUG):
        yield


async def recorded_history(calls: int = 1):
    """Run `calls` recorded operations against a fresh operation history and return what they recorded."""
    appender = FakeAppender()
    ctx = make_context(appender=appender)
    for _ in range(calls):
        await ctx.now()
    return appender.events


async def test_fresh_execution_emits_workflow_logs(caplog) -> None:
    ctx = make_context()

    ctx.logger.info("started")

    assert [r.getMessage() for r in caplog.records] == ["started"]


async def test_replaying_execution_suppresses_workflow_logs(caplog) -> None:
    history = await recorded_history()
    ctx = make_context(history)

    ctx.logger.info("started")

    assert caplog.records == []


async def test_logs_resume_once_the_replay_prefix_is_exhausted(caplog) -> None:
    history = await recorded_history()
    ctx = make_context(history)

    ctx.logger.info("before replayed call")
    await ctx.now()
    ctx.logger.info("after replayed call")

    assert [r.getMessage() for r in caplog.records] == ["after replayed call"]


async def test_only_the_prefix_is_suppressed_when_more_history_remains(caplog) -> None:
    history = await recorded_history(calls=2)
    ctx = make_context(history)

    await ctx.now()
    ctx.logger.info("mid replay")
    await ctx.now()
    ctx.logger.info("new work")

    assert [r.getMessage() for r in caplog.records] == ["new work"]


async def test_every_level_is_supported(caplog) -> None:
    ctx = make_context()

    ctx.logger.debug("d")
    ctx.logger.info("i")
    ctx.logger.warning("w")
    ctx.logger.error("e")
    ctx.logger.critical("c")
    ctx.logger.log(logging.INFO, "l")

    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.DEBUG, "d"),
        (logging.INFO, "i"),
        (logging.WARNING, "w"),
        (logging.ERROR, "e"),
        (logging.CRITICAL, "c"),
        (logging.INFO, "l"),
    ]


async def test_formatting_arguments_are_forwarded(caplog) -> None:
    ctx = make_context()

    ctx.logger.info("greeting %s number %d", "world", 2)

    assert caplog.records[0].getMessage() == "greeting world number 2"


async def test_exception_logging_carries_the_active_exception(caplog) -> None:
    ctx = make_context()

    try:
        raise ValueError("boom")  # noqa: TRY301
    except ValueError:
        ctx.logger.exception("step blew up")

    record = caplog.records[0]
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], ValueError)


async def test_records_carry_run_metadata(caplog) -> None:
    ctx = make_context()

    ctx.logger.info("started", extra={"custom": "value"})

    record = caplog.records[0]
    assert record.workflow_id == DEFAULT_RUN_INFO.wf_id
    assert record.run_id == DEFAULT_RUN_INFO.id
    assert record.workflow_type == DEFAULT_RUN_INFO.wf_type
    assert record.step_name == "current_step"
    assert record.worker_id == DEFAULT_WORKER_ID
    assert record.custom == "value"


async def test_records_are_attributed_to_the_calling_step(caplog) -> None:
    ctx = make_context()

    ctx.logger.info("started")

    assert caplog.records[0].funcName == "test_records_are_attributed_to_the_calling_step"


async def test_sdk_loggers_still_emit_while_a_step_is_replaying(caplog) -> None:
    history = await recorded_history()
    ctx = make_context(history)

    ctx.logger.info("workflow log")
    get_logger("grctl.exec.task").warning("transport degraded")

    assert [r.getMessage() for r in caplog.records] == ["transport degraded"]


class DirectiveSink:
    """Swallows the directives an execution publishes — this file asserts on logs, not directives."""

    async def send(self, directive: Directive) -> None:
        return None


async def run_step(step_history: list[HistoryEvent], appender: FakeAppender) -> None:
    """Run one step through a real Execution, so the logger comes from the production wiring."""

    async def handler(ctx: Context) -> Directive:
        ctx.logger.info("before recorded call")
        await ctx.now()
        ctx.logger.info("after recorded call")
        return ctx.next.complete("ok")

    codec = MsgspecCodec()
    workflow_api = FakeWorkflowAPI()
    execution = Execution(
        WorkerInfo(id=DEFAULT_WORKER_ID, name="test-worker"),
        Directive(
            id="directive-1",
            timestamp=DEFAULT_RUN_INFO.created_at,
            kind=DirectiveKind.step,
            run_info=DEFAULT_RUN_INFO,
            msg=Step(step_name="current_step"),
        ),
        RegisteredStep(handler=handler, spec=HandlerSpec(payload_parameters={})),
        ExecutionDeps(
            kvman=KVManager(FakeKVApi(), codec),
            history_appender=appender,
            directive_api=DirectiveSink(),
            codec=codec,
            workflow_api=workflow_api,
            handle_factory=WorkflowHandleFactory(
                workflow_api=workflow_api,
                listener_factory=FakeHistoryListenerFactory(),
                sender_id=DEFAULT_WORKER_ID,
                logger=get_logger("tests.exec"),
                decoder=codec,
            ),
            step_history=step_history,
            step_infos={"current_step": StepInfo(timeout_ms=0)},
        ),
        get_logger("tests.exec"),
    )

    await execution.execute()


async def test_a_retried_step_only_logs_past_its_recorded_history(caplog) -> None:
    """The end-to-end shape of the fix: crash after a recorded call, then re-run the step."""
    first_attempt = FakeAppender()
    await run_step([], first_attempt)

    assert [r.getMessage() for r in caplog.records] == ["before recorded call", "after recorded call"]

    caplog.clear()
    await run_step(first_attempt.events, FakeAppender())

    assert [r.getMessage() for r in caplog.records] == ["after recorded call"]


async def test_replay_state_is_per_execution(caplog) -> None:
    history = await recorded_history()
    replaying = make_context(history)
    fresh = make_context()

    replaying.logger.info("from replaying run")
    fresh.logger.info("from fresh run")

    assert [r.getMessage() for r in caplog.records] == ["from fresh run"]
