import pytest

from grctl.models.directive import Directive
from grctl.worker.context import Context
from grctl.workflow.workflow import HandlerSpec, Workflow, inspect_handler


async def _handler_with_typed_params(ctx: Context, name: str, count: int) -> Directive: ...  # ty:ignore[empty-body]


async def _handler_ctx_only(ctx: Context) -> Directive: ...  # ty:ignore[empty-body]


async def _handler_unannotated(ctx: Context, name) -> Directive: ...  # ty:ignore[empty-body]


async def _handler_varargs(ctx: Context, *args: str) -> Directive: ...  # ty:ignore[empty-body]


async def _handler_kwargs(ctx: Context, **kwargs: str) -> Directive: ...  # ty:ignore[empty-body]


def test_typed_params_captured():
    spec = inspect_handler(_handler_with_typed_params)

    assert isinstance(spec, HandlerSpec)
    assert spec.params == {"name": str, "count": int}


def test_ctx_only_yields_empty_params():
    spec = inspect_handler(_handler_ctx_only)

    assert spec.params == {}


def test_missing_annotation_raises_at_registration():
    wf = Workflow(workflow_type="test")

    with pytest.raises(TypeError, match="must have a type annotation"):
        wf.start()(_handler_unannotated)


def test_varargs_raises_at_registration():
    wf = Workflow(workflow_type="test")

    with pytest.raises(TypeError, match="must not use"):
        wf.start()(_handler_varargs)


def test_kwargs_raises_at_registration():
    wf = Workflow(workflow_type="test")

    with pytest.raises(TypeError, match="must not use"):
        wf.start()(_handler_kwargs)
