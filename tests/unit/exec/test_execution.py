"""Execution outcomes for failures before a handler is invoked."""

import logging
from typing import Any

from grctl.exec.execution import Execution, ExecutionDeps
from grctl.exec.kv_manager import KVManager
from grctl.models import Directive, DirectiveKind, FailStep, Step, StepResult
from grctl.models.handler import HandlerConfig, HandlerSpec
from grctl.models.worker import WorkerInfo
from grctl.workflow.handle import WorkflowHandleFactory
from grctl.workflow.workflow import StepInfo
from tests.unit.exec.fakes import (
    DEFAULT_RUN_INFO,
    DEFAULT_WORKER_ID,
    FakeAppender,
    FakeHistoryListenerFactory,
    FakeKVApi,
    FakeWorkflowAPI,
)


class FailingCodec:
    """Rejects malformed inbound handler payloads."""

    def to_primitive(self, value: Any) -> Any:
        return value

    def from_primitive(self, raw: Any, tp: type | None = None) -> Any:
        raise TypeError(f"Cannot convert {raw!r} to {tp!r}")

    def cast(self, value: Any, ty: type | None = None) -> Any:
        return self.from_primitive(value, ty)


class DirectiveRecorder:
    """Collects directives that an execution publishes."""

    def __init__(self) -> None:
        self.directives: list[Directive] = []

    async def send(self, directive: Directive) -> None:
        self.directives.append(directive)


async def test_invalid_handler_payload_sends_a_failed_step_result() -> None:
    """Payload conversion failures are durable workflow failures, not task crashes."""

    async def handler(_ctx: object, _count: int) -> Directive:
        raise AssertionError("Handler must not run when its payload cannot be converted")

    codec = FailingCodec()
    recorder = DirectiveRecorder()
    directive = Directive(
        id="directive-1",
        timestamp=DEFAULT_RUN_INFO.created_at,
        kind=DirectiveKind.step,
        run_info=DEFAULT_RUN_INFO,
        msg=Step(step_name="current_step", payload="not-an-int"),
    )
    worker = WorkerInfo(id=DEFAULT_WORKER_ID, name="test-worker")
    workflow_api = FakeWorkflowAPI()
    listener_factory = FakeHistoryListenerFactory()
    execution = Execution(
        worker,
        directive,
        HandlerConfig(handler=handler, spec=HandlerSpec(params={"count": int})),
        ExecutionDeps(
            kvman=KVManager(FakeKVApi(), codec),
            history_appender=FakeAppender(),
            directive_api=recorder,
            codec=codec,
            workflow_api=workflow_api,
            handle_factory=WorkflowHandleFactory(
                workflow_api=workflow_api,
                listener_factory=listener_factory,
                sender_id=DEFAULT_WORKER_ID,
                logger=logging.getLogger("tests.exec"),
                decoder=codec,
            ),
            step_history=[],
            step_infos={"current_step": StepInfo(timeout_ms=0)},
        ),
        logging.getLogger("tests.exec"),
    )

    await execution.execute()

    assert len(recorder.directives) == 2
    result = recorder.directives[-1].msg
    assert recorder.directives[-1].kind is DirectiveKind.step_result
    assert isinstance(result, StepResult)
    assert isinstance(result.next_msg, FailStep)
    assert result.next_msg.step_name == "current_step"
    assert result.next_msg.error.type == "TypeError"
