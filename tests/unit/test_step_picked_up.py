"""Tests for StepPickedUp directive published by the worker at step pick-up time."""

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import msgspec

from grctl.logging_config import setup_logging
from grctl.models import Directive, DirectiveKind, HistoryEvent, HistoryKind, RunInfo, Start, Step
from grctl.models.directive import StepPickedUp
from grctl.nats.connection import Connection
from grctl.worker.context import Context
from grctl.worker.run_manager import RunManager
from grctl.worker.runtime import StepRuntime, set_step_runtime
from grctl.workflow import Workflow
from tests.conftest import create_directive

setup_logging(level=logging.DEBUG)


def _make_history_event(kind: HistoryKind, msg: object, operation_id: str) -> HistoryEvent:
    return HistoryEvent(
        wf_id="wf-1",
        run_id="run-1",
        worker_id="w-1",
        timestamp=datetime.now(UTC),
        kind=kind,
        msg=msg,
        operation_id=operation_id,
    )


def _make_runtime(workflow: Workflow, step_history: list[HistoryEvent] | None = None) -> StepRuntime:
    runtime = StepRuntime(
        workflow=workflow,
        worker_id="worker-abc",
        directive=Mock(spec=Directive),
        connection=AsyncMock(spec=Connection),
        step_history=step_history if step_history is not None else [],
    )
    runtime.publisher.publish_history = AsyncMock()  # ty:ignore[invalid-assignment]
    runtime.publisher.publish_next_directive = AsyncMock()  # ty:ignore[invalid-assignment]
    runtime.step_name = "my_step"
    return runtime


class TestStepPickedUpModel:
    def test_encodes_and_decodes_via_msgpack(self) -> None:
        now = datetime.now(UTC)
        original = StepPickedUp(step_name="my_step", worker_id="worker-1", timestamp=now)

        encoded = msgspec.msgpack.encode(original)
        decoded = msgspec.msgpack.decode(encoded, type=StepPickedUp)

        assert decoded.step_name == "my_step"
        assert decoded.worker_id == "worker-1"
        assert decoded.timestamp == now

    def test_kind_value(self) -> None:
        assert DirectiveKind.step_picked_up == "step_picked_up"

    def test_roundtrip_via_directive_encoder_decoder(self) -> None:
        from grctl.models import directive_decoder, directive_encoder  # noqa: PLC0415

        now = datetime.now(UTC)
        directive = Directive(
            id="test-id",
            timestamp=now,
            kind=DirectiveKind.step_picked_up,
            run_info=RunInfo(id="run-1", wf_id="wf-1", wf_type="TestWf", created_at=now),
            msg=StepPickedUp(step_name="s", worker_id="w", timestamp=now),
        )

        decoded = directive_decoder(directive_encoder(directive))

        assert decoded.kind == DirectiveKind.step_picked_up
        assert isinstance(decoded.msg, StepPickedUp)
        assert decoded.msg.step_name == "s"
        assert decoded.msg.worker_id == "w"


class TestStepPickedUpPublishing:
    async def test_publishes_before_handler_on_first_execution(self, mock_connection) -> None:
        connection, published = mock_connection

        wf = Workflow(workflow_type="PickupTest")
        handler_called_after_pickup: list[bool] = []

        @wf.start()
        async def start(ctx: Context) -> Directive:
            pickup_directives = [
                msg
                for _, msg in published
                if isinstance(msg, Directive) and msg.kind == DirectiveKind.step_picked_up
            ]
            handler_called_after_pickup.append(len(pickup_directives) > 0)
            return ctx.next.complete(None)

        manager = RunManager(
            worker_name="test-worker",
            worker_id="worker-xyz",
            workflows=[wf],
            connection=connection,
        )

        directive = create_directive(
            kind=DirectiveKind.start,
            msg=Start(),
            directive_id="dir-1",
            run_id="run-1",
            wf_id="wf-1",
            wf_type="PickupTest",
        )

        await manager.handle_next_directive(directive)
        await manager.shutdown()

        assert handler_called_after_pickup == [True], "StepPickedUp must be published before the handler runs"

    async def test_payload_correctness(self, mock_connection) -> None:
        connection, published = mock_connection

        wf = Workflow(workflow_type="PayloadTest")

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.complete(None)

        manager = RunManager(
            worker_name="test-worker",
            worker_id="worker-xyz",
            workflows=[wf],
            connection=connection,
        )

        directive = create_directive(
            kind=DirectiveKind.start,
            msg=Start(),
            directive_id="dir-2",
            run_id="run-2",
            wf_id="wf-2",
            wf_type="PayloadTest",
        )

        before = datetime.now(UTC)
        await manager.handle_next_directive(directive)
        await manager.shutdown()
        after = datetime.now(UTC)

        pickup = next(
            (msg for _, msg in published if isinstance(msg, Directive) and msg.kind == DirectiveKind.step_picked_up),
            None,
        )
        assert pickup is not None, "Expected a StepPickedUp directive"
        assert isinstance(pickup.msg, StepPickedUp)
        assert pickup.msg.step_name == "start"
        assert pickup.msg.worker_id == "worker-xyz"
        assert before <= pickup.msg.timestamp <= after

    async def test_suppressed_on_replay(self) -> None:
        from grctl.models.history import TaskCompleted  # noqa: PLC0415
        from grctl.worker.runner import WorkflowRunner  # noqa: PLC0415

        wf = Workflow(workflow_type="ReplayTest")

        @wf.step()
        async def my_step(ctx: Context) -> Directive:
            return ctx.next.complete(None)

        runtime = _make_runtime(
            workflow=wf,
            step_history=[
                _make_history_event(
                    HistoryKind.task_completed,
                    TaskCompleted(
                        task_id="op:abc",
                        task_name="some_task",
                        step_name="my_step",
                        output={"result": None},
                        duration_ms=5,
                    ),
                    "op:abc",
                )
            ],
        )
        set_step_runtime(runtime)

        runner = WorkflowRunner(runtime)

        step_msg = Step(step_name="my_step")
        await runner.handle_step(step_msg)

        calls = runtime.publisher.publish_next_directive.call_args_list  # ty:ignore[unresolved-attribute]
        pickup_calls = [
            c for c in calls if isinstance(c.args[1], Directive) and c.args[1].kind == DirectiveKind.step_picked_up
        ]
        assert pickup_calls == [], f"StepPickedUp must not be published on replay; got {pickup_calls}"

    async def test_no_step_started_history_event_published(self, mock_connection) -> None:
        connection, published = mock_connection

        wf = Workflow(workflow_type="NoHistoryTest")

        @wf.start()
        async def start(ctx: Context) -> Directive:
            return ctx.next.complete(None)

        manager = RunManager(
            worker_name="test-worker",
            worker_id="worker-xyz",
            workflows=[wf],
            connection=connection,
        )

        directive = create_directive(
            kind=DirectiveKind.start,
            msg=Start(),
            directive_id="dir-3",
            run_id="run-3",
            wf_id="wf-3",
            wf_type="NoHistoryTest",
        )

        await manager.handle_next_directive(directive)
        await manager.shutdown()

        history_events = [msg for subject, msg in published if "grctl_history" in subject and not isinstance(msg, str)]
        step_started_events = [e for e in history_events if e.kind == HistoryKind.step_started]
        assert step_started_events == [], f"Worker must not publish step.started history; got {step_started_events}"
