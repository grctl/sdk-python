from datetime import timedelta

import pytest

from grctl.models import Complete, Directive, DirectiveKind, ErrorDetails, Fail, Step, Wait
from grctl.workflow.workflow import StepInfo

from .fakes import make_context


async def next_step(ctx: object) -> Directive:
    raise AssertionError(ctx)


async def timeout_step(ctx: object) -> Directive:
    raise AssertionError(ctx)


async def renamed_handler(ctx: object) -> Directive:
    raise AssertionError(ctx)


renamed_handler.__grctl_step_name__ = "registered_name"  # ty:ignore[unresolved-attribute]


class UnnamedHandler:
    async def __call__(self) -> Directive:
        raise AssertionError


def test_next_step_uses_the_handler_name() -> None:
    directive = make_context(step_infos={"next_step": StepInfo(timeout_ms=1_250)}).next.step(next_step)

    assert directive.kind is DirectiveKind.step
    assert directive.msg == Step(step_name="next_step", timeout_ms=1_250)


def test_next_wait_sets_timeout_and_callback_name() -> None:
    directive = make_context(step_infos={"timeout_step": StepInfo(timeout_ms=0)}).next.wait(
        timedelta(seconds=2.5), timeout_step
    )

    assert directive.kind is DirectiveKind.wait
    assert directive.msg == Wait(timeout_ms=2500, timeout_step_name="timeout_step")


def test_next_complete_preserves_the_result() -> None:
    result = {"status": "done"}

    directive = make_context().next.complete(result)

    assert directive.kind is DirectiveKind.complete
    assert directive.msg == Complete(result=result)


def test_next_fail_preserves_error_details() -> None:
    error = ErrorDetails(type="PaymentDeclined", message="card rejected", stack_trace="")

    directive = make_context().next.fail(error)

    assert directive.kind is DirectiveKind.fail
    assert directive.msg == Fail(error=error)


def test_next_step_requires_a_named_handler() -> None:
    with pytest.raises(ValueError, match="__name__"):
        make_context().next.step(UnnamedHandler())


def test_next_step_rejects_an_unregistered_handler() -> None:
    with pytest.raises(ValueError, match="not registered"):
        make_context().next.step(next_step)


def test_next_step_uses_the_registered_name_and_timeout() -> None:
    directive = make_context(
        step_infos={"registered_name": StepInfo(timeout_ms=4_000)},
    ).next.step(renamed_handler)

    assert directive.msg == Step(step_name="registered_name", timeout_ms=4_000)


def test_next_wait_uses_the_registered_callback_name() -> None:
    directive = make_context(
        step_infos={"registered_name": StepInfo(timeout_ms=0)},
    ).next.wait(on_timeout=renamed_handler)

    assert directive.msg == Wait(timeout_step_name="registered_name")


def test_child_callback_uses_the_registered_name() -> None:
    ctx = make_context(step_infos={"registered_name": StepInfo(timeout_ms=0)})

    assert ctx._callback_step_name(renamed_handler) == "registered_name"
