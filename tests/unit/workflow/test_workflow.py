from datetime import timedelta
from typing import Any

import pytest

from grctl.models.handler import StepKind
from grctl.workflow.workflow import Workflow, get_handler_spec


def make_workflow() -> Workflow:
    return Workflow(workflow_type="test-workflow")


class Ctx:
    """Stand-in for RunContext; only used as the skipped first parameter."""


async def test_workflow_type_returns_the_configured_type() -> None:
    assert make_workflow().workflow_type == "test-workflow"


async def test_step_defaults_to_an_internal_step() -> None:
    wf = make_workflow()

    @wf.step()
    async def my_step(ctx: Ctx, name: str) -> Any:
        return name

    assert wf.step_names == ["my_step"]
    assert wf.step_handler("my_step").kind is StepKind.internal
    assert wf.start_step_name is None
    assert wf.event_names == []


async def test_start_step_is_registered_in_the_shared_namespace() -> None:
    wf = make_workflow()

    @wf.step(start=True)
    async def begin(ctx: Ctx, order_id: str) -> Any:
        return order_id

    config = wf.step_handler("begin")
    assert wf.start_step_name == "begin"
    assert config.kind is StepKind.start
    assert wf.type_def().step_defs[0].name == "begin"
    assert wf.type_def().step_defs[0].timeout_ms == 0
    assert wf.type_def().step_defs[0].external is False


async def test_only_one_start_step_is_allowed() -> None:
    wf = make_workflow()

    @wf.step(start=True)
    async def first(ctx: Ctx) -> Any:
        return None

    with pytest.raises(ValueError, match="already has a start handler"):

        @wf.step(start=True)
        async def second(ctx: Ctx) -> Any:
            return None


async def test_start_and_event_flags_cannot_be_combined() -> None:
    wf = make_workflow()

    with pytest.raises(ValueError, match="both a start step and an event step"):

        @wf.step(start=True, event=True)
        async def invalid(ctx: Ctx) -> Any:
            return None


async def test_event_step_defaults_to_function_name_and_is_external() -> None:
    wf = make_workflow()

    @wf.step(event=True)
    async def approve(ctx: Ctx) -> Any:
        return None

    assert wf.event_names == ["approve"]
    assert wf.step_handler("approve").kind is StepKind.external
    assert wf.type_def().step_defs[0].external is True


async def test_event_step_explicit_name_registers_only_that_name() -> None:
    wf = make_workflow()

    @wf.step(event=True, name="approved")
    async def handler(ctx: Ctx) -> Any:
        return None

    assert wf.step_names == ["approved"]
    assert wf.event_names == ["approved"]
    assert wf.step_handler("approved").handler is handler
    assert handler.__grctl_step_name__ == "approved"  # ty:ignore[unresolved-attribute]
    with pytest.raises(ValueError, match="not registered"):
        wf.step_handler("handler")


async def test_duplicate_names_are_rejected_across_all_step_kinds() -> None:
    wf = make_workflow()

    @wf.step(event=True, name="approved")
    async def event_handler(ctx: Ctx) -> Any:
        return None

    with pytest.raises(ValueError, match="already registered"):

        @wf.step()
        async def approved(ctx: Ctx) -> Any:
            return None


async def test_timeout_none_registers_zero_and_custom_timeout_is_serialized() -> None:
    wf = make_workflow()

    @wf.step()
    async def untimed(ctx: Ctx) -> Any:
        return None

    @wf.step(timeout=timedelta(seconds=30))
    async def timed(ctx: Ctx) -> Any:
        return None

    defs = {definition.name: definition.timeout_ms for definition in wf.type_def().step_defs}
    assert defs == {"untimed": 0, "timed": 30_000}
    assert wf.step_handler("untimed").timeout is None


async def test_old_start_and_event_decorators_are_removed() -> None:
    wf = make_workflow()

    assert not hasattr(wf, "start")
    assert not hasattr(wf, "event")
    assert not hasattr(wf, "start_handler")
    assert not hasattr(wf, "_step_handlers")
    assert not hasattr(wf, "_on_event_handlers")


async def test_query_defaults_to_the_function_name() -> None:
    wf = make_workflow()

    @wf.query()
    async def get_status(ctx: Ctx) -> str:
        return "running"

    assert wf.query_names == ["get_status"]


async def test_registering_a_duplicate_query_name_raises() -> None:
    wf = make_workflow()

    @wf.query(name="status")
    async def first(ctx: Ctx) -> str:
        return "a"

    with pytest.raises(ValueError, match="already registered"):

        @wf.query(name="status")
        async def second(ctx: Ctx) -> str:
            return "b"


async def test_get_handler_spec_skips_the_first_parameter_and_resolves_types() -> None:
    async def handler(ctx: Ctx, name: str, count: int) -> None:
        return None

    assert get_handler_spec(handler).params == {"name": str, "count": int}


async def test_get_handler_spec_raises_without_a_type_annotation() -> None:
    async def handler(ctx: Ctx, name) -> None:
        return None

    with pytest.raises(TypeError, match="must have a type annotation"):
        get_handler_spec(handler)


async def test_get_handler_spec_rejects_var_positional_args() -> None:
    async def handler(ctx: Ctx, *args: str) -> None:
        return None

    with pytest.raises(TypeError, match=r"\*args or \*\*kwargs"):
        get_handler_spec(handler)


async def test_get_handler_spec_rejects_var_keyword_args() -> None:
    async def handler(ctx: Ctx, **kwargs: str) -> None:
        return None

    with pytest.raises(TypeError, match=r"\*args or \*\*kwargs"):
        get_handler_spec(handler)
