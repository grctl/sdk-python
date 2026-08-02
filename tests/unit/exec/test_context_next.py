from datetime import timedelta

import pytest

from grctl.models import Complete, Directive, DirectiveKind, ErrorDetails, Fail, Step, Wait

from .fakes import make_context


async def next_step(ctx: object) -> Directive:
    raise AssertionError(ctx)


async def timeout_step(ctx: object) -> Directive:
    raise AssertionError(ctx)


class UnnamedHandler:
    async def __call__(self) -> Directive:
        raise AssertionError


def test_next_step_uses_the_handler_name() -> None:
    directive = make_context().next.step(next_step)

    assert directive.kind is DirectiveKind.step
    assert directive.msg == Step(step_name="next_step")


def test_next_wait_sets_timeout_and_callback_name() -> None:
    directive = make_context().next.wait(timedelta(seconds=2.5), timeout_step)

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
