"""Shared workflow definitions for spec tests.

These workflows cover common patterns reused across multiple spec test files.
Test-specific workflows should be defined inline within the test file.
"""

from grctl.worker import Context, task
from grctl.workflow import Directive, Workflow

# --- simple_wf ---
# Single-step workflow. Calls one task and returns its result.
# Used for basic end-to-end verification and as a base for history/replay tests.

simple_wf = Workflow(workflow_type="spec_simple")


@task
async def echo_task(value: str) -> str:
    return value


@simple_wf.start()
async def simple_start(ctx: Context, value: str) -> Directive:
    result = await echo_task(value)
    return ctx.next.complete(result)
