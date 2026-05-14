"""Shared workflow definitions for spec tests.

These workflows cover common patterns reused across multiple spec test files.
Test-specific workflows should be defined inline within the test file.
"""

import asyncio
from datetime import timedelta
from typing import Any

import ulid

from grctl.worker import Context, task
from grctl.workflow import Directive, Workflow


def unique_workflow_type(prefix: str) -> str:
    return f"{prefix}_{str(ulid.ULID()).lower()}"


# ─── Factories ────────────────────────────────────────────────────────────────


def make_completing_workflow(result: Any = "ok", prefix: str = "spec_completing") -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type(prefix))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.complete(result)

    return wf


def make_failing_workflow(message: str = "step exploded", prefix: str = "spec_failing") -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type(prefix))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        raise ValueError(message)

    return wf


def make_echo_workflow(prefix: str = "spec_echo") -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type(prefix))

    @wf.start()
    async def start(ctx: Context, value: str) -> Directive:
        result = await echo_task(value)
        return ctx.next.complete(result)

    return wf


def make_waiting_event_workflow(
    event_timeout: timedelta = timedelta(seconds=30),
    prefix: str = "spec_waiting_event",
) -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type(prefix))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.wait_for_event(timeout=event_timeout)

    @wf.event()
    async def finish(ctx: Context, result: str = "done") -> Directive:
        return ctx.next.complete(result)

    return wf


def make_two_step_workflow(prefix: str = "spec_two_step") -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type(prefix))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(second_step)

    @wf.step()
    async def second_step(ctx: Context) -> Directive:
        return ctx.next.complete("two-step-ok")

    return wf


def make_blocking_step_workflow(
    step_timeout: timedelta = timedelta(seconds=0.1),
    prefix: str = "spec_blocking_step",
) -> Workflow:
    wf = Workflow(workflow_type=unique_workflow_type(prefix))

    @wf.start()
    async def start(ctx: Context) -> Directive:
        return ctx.next.step(blocking_step)

    @wf.step(timeout=step_timeout)
    async def blocking_step(ctx: Context) -> Directive:
        await asyncio.sleep(60)
        return ctx.next.complete("unreachable")

    return wf


# ─── Static shared workflows ──────────────────────────────────────────────────
# Fixed workflow types kept for tests that assert on specific step/function names.
# Prefer factories for new tests.


@task
async def echo_task(value: str) -> str:
    return value


two_step_wf = Workflow(workflow_type="spec_step_two_step")


@two_step_wf.start()
async def two_step_start(ctx: Context) -> Directive:
    return ctx.next.step(two_step_second)


@two_step_wf.step()
async def two_step_second(ctx: Context) -> Directive:
    return ctx.next.complete("two-step-ok")
