from datetime import timedelta

import msgspec

from grctl.models.command import EventDef, WorkflowTypeDef
from grctl.models.directive import Directive
from grctl.worker.registration import build_catalog
from grctl.workflow.workflow import Workflow


def test_eventdef_round_trips_through_msgpack() -> None:
    orig = EventDef(name="ev", timeout_ms=100)
    data = msgspec.msgpack.encode(orig)
    restored = msgspec.msgpack.decode(data, type=EventDef)
    assert restored == orig


def test_eventdef_defaults_to_zero_values() -> None:
    d = EventDef(name="ev")
    assert d.timeout_ms == 0


def test_workflowtypedef_with_eventdef_round_trips() -> None:
    orig = WorkflowTypeDef(
        type="t",
        start_step="s",
        steps=["s"],
        events=[EventDef(name="e", timeout_ms=100)],
        queries=["q"],
        start_step_timeout_ms=0,
    )
    data = msgspec.msgpack.encode(orig)
    restored = msgspec.msgpack.decode(data, type=WorkflowTypeDef)
    assert restored == orig


def test_event_defs_with_timeout_only() -> None:
    wf = Workflow(workflow_type="test")

    @wf.event(timeout=timedelta(milliseconds=100))
    async def my_event(ctx) -> Directive:
        raise NotImplementedError

    defs = wf.event_defs
    assert len(defs) == 1
    assert defs[0] == EventDef(name="my_event", timeout_ms=100)


def test_event_defs_with_no_timeout() -> None:
    wf = Workflow(workflow_type="test")

    @wf.event()
    async def my_event(ctx) -> Directive:
        raise NotImplementedError

    defs = wf.event_defs
    assert len(defs) == 1
    assert defs[0] == EventDef(name="my_event")


def test_event_defs_multiple_events() -> None:
    wf = Workflow(workflow_type="test")

    @wf.event()
    async def ev1(ctx) -> Directive:
        raise NotImplementedError

    @wf.event(timeout=timedelta(seconds=2))
    async def ev2(ctx) -> Directive:
        raise NotImplementedError

    async def on_to(ctx) -> Directive:
        raise NotImplementedError

    @wf.event(name="ev3", timeout=timedelta(seconds=5))
    async def ev3_handler(ctx) -> Directive:
        raise NotImplementedError

    defs = wf.event_defs
    assert len(defs) == 3

    by_name = {d.name: d for d in defs}
    assert by_name["ev1"] == EventDef(name="ev1")
    assert by_name["ev2"] == EventDef(name="ev2", timeout_ms=2000)
    assert by_name["ev3"] == EventDef(name="ev3", timeout_ms=5000)


def test_to_type_def_includes_event_defs() -> None:
    wf = Workflow(workflow_type="order_wf")

    @wf.start()
    async def start(ctx, order_id: str) -> Directive:
        raise NotImplementedError

    async def on_to(ctx) -> Directive:
        raise NotImplementedError

    @wf.event(name="approve", timeout=timedelta(seconds=30))
    async def on_approve(ctx) -> Directive:
        raise NotImplementedError

    catalog = build_catalog([wf])
    assert len(catalog) == 1
    assert catalog[0].events == [EventDef(name="approve", timeout_ms=30000)]
