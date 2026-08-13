from grctl.exec.drc_factory import DrcFactory
from grctl.models import Directive, DirectiveKind, ErrorDetails, Fail, FailStep, Step
from grctl.workflow.workflow import StepInfo

from .fakes import DEFAULT_RUN_INFO, DEFAULT_WORKER_ID

ERROR = ErrorDetails(type="ValueError", message="card rejected", stack_trace="")


def make_factory() -> DrcFactory:
    directive = Directive(
        id="directive-1",
        timestamp=DEFAULT_RUN_INFO.created_at,
        kind=DirectiveKind.step,
        run_info=DEFAULT_RUN_INFO,
        msg=Step(step_name="current_step"),
    )
    return DrcFactory(DEFAULT_RUN_INFO, DEFAULT_WORKER_ID, directive, {"current_step": StepInfo(timeout_ms=0)})


def test_fail_reports_a_handler_decided_run_failure() -> None:
    directive = make_factory().fail(ERROR)

    assert directive.kind is DirectiveKind.fail
    assert directive.msg == Fail(error=ERROR)


def test_fail_step_reports_the_raising_step() -> None:
    """The server records step.failed only for fail_step; a plain fail records the step as completed."""
    directive = make_factory().fail_step("current_step", ERROR)

    assert directive.kind is DirectiveKind.fail_step
    assert directive.msg == FailStep(step_name="current_step", error=ERROR)
