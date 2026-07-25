from datetime import timedelta
from typing import Any

import pytest

from grctl.workflow.workflow import Workflow, get_handler_spec


def make_workflow() -> Workflow:
    return Workflow(workflow_type="test-workflow")


class Ctx:
    """Stand-in for RunContext; only used as the skipped first parameter."""


async def test_workflow_type_returns_the_configured_type() -> None:
    wf = make_workflow()

    assert wf.workflow_type == "test-workflow"


async def test_start_step_name_is_none_until_a_start_handler_is_registered() -> None:
    wf = make_workflow()

    assert wf.start_step_name is None

    @wf.start()
    async def begin(ctx: Ctx, order_id: str) -> Any:
        return order_id

    assert wf.start_step_name == "begin"


async def test_registering_a_second_start_handler_raises() -> None:
    wf = make_workflow()

    @wf.start()
    async def first(ctx: Ctx) -> Any:
        return None

    with pytest.raises(ValueError, match="already has a start handler"):

        @wf.start()
        async def second(ctx: Ctx) -> Any:
            return None


async def test_step_handlers_default_to_a_ten_second_timeout() -> None:
    wf = make_workflow()

    @wf.step()
    async def my_step(ctx: Ctx, name: str) -> Any:
        return name

    assert wf.step_names == ["my_step"]
    assert wf._step_handlers["my_step"].timeout == timedelta(seconds=10)  # noqa: SLF001


async def test_step_handler_honors_a_custom_timeout() -> None:
    wf = make_workflow()

    @wf.step(timeout=timedelta(seconds=30))
    async def my_step(ctx: Ctx) -> Any:
        return None

    assert wf._step_handlers["my_step"].timeout == timedelta(seconds=30)  # noqa: SLF001


async def test_registering_a_duplicate_step_name_raises() -> None:
    wf = make_workflow()

    @wf.step()
    async def my_step(ctx: Ctx) -> Any:
        return None

    with pytest.raises(ValueError, match="already registered"):

        @wf.step()
        async def my_step(ctx: Ctx) -> Any:  # noqa: F811
            return None


async def test_event_defaults_to_the_function_name() -> None:
    wf = make_workflow()

    @wf.event()
    async def approve(ctx: Ctx) -> Any:
        return None

    assert wf.event_names == ["approve"]


async def test_event_can_be_registered_under_a_custom_name() -> None:
    wf = make_workflow()

    @wf.event(name="custom_event")
    async def handler(ctx: Ctx) -> Any:
        return None

    assert wf.event_names == ["custom_event"]


async def test_registering_a_duplicate_event_name_raises() -> None:
    wf = make_workflow()

    @wf.event(name="approve")
    async def first(ctx: Ctx) -> Any:
        return None

    with pytest.raises(ValueError, match="already registered"):

        @wf.event(name="approve")
        async def second(ctx: Ctx) -> Any:
            return None


async def test_event_defs_reports_timeout_ms_derived_from_timedelta() -> None:
    wf = make_workflow()

    @wf.event(timeout=timedelta(seconds=2))
    async def timed_event(ctx: Ctx) -> Any:
        return None

    @wf.event()
    async def untimed_event(ctx: Ctx) -> Any:
        return None

    defs = {d.name: d.timeout_ms for d in wf.event_defs}
    assert defs == {"timed_event": 2000, "untimed_event": 0}


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

    spec = get_handler_spec(handler)

    assert spec.params == {"name": str, "count": int}


async def test_get_handler_spec_raises_without_a_type_annotation() -> None:
    async def handler(ctx: Ctx, name) -> None:  # noqa: ANN001
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
