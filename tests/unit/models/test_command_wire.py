import msgspec

from grctl.models import Directive
from grctl.models.command import StepDef, WorkflowTypeDef
from grctl.models.directive import Start, Step
from grctl.models.handler import HandlerConfig, HandlerSpec, StepKind


def test_workflow_type_def_uses_step_defs_wire_shape() -> None:
    definition = WorkflowTypeDef(
        type="payments",
        start_step="begin",
        step_defs=[
            StepDef(name="begin", timeout_ms=1_000),
            StepDef(name="approved", timeout_ms=2_000, external=True),
        ],
        queries=["status"],
    )

    encoded = msgspec.msgpack.encode(definition)
    decoded = msgspec.msgpack.decode(encoded, type=WorkflowTypeDef)

    assert decoded == definition
    assert msgspec.msgpack.decode(encoded) == {
        "type": "payments",
        "start_step": "begin",
        "step_defs": [
            {"name": "begin", "timeout_ms": 1_000, "external": False},
            {"name": "approved", "timeout_ms": 2_000, "external": True},
        ],
        "queries": ["status"],
    }


def test_directive_timeouts_default_to_server_defaults() -> None:
    assert Start().timeout_ms == 0
    assert Step(step_name="begin").timeout_ms == 0


async def handler() -> Directive:
    raise NotImplementedError


def test_handler_config_has_step_kind_and_no_timeout_handler() -> None:
    config = HandlerConfig(
        handler=handler,
        spec=HandlerSpec(params={}),
        kind=StepKind.external,
    )

    assert config.kind is StepKind.external
    assert not hasattr(config, "on_timeout_handler")
