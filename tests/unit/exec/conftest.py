from collections.abc import Callable

import pytest

from grctl.exec.context import Context
from grctl.exec.task import set_current_context


@pytest.fixture
def in_step() -> Callable[[Context], Context]:
    """Enter a step context so `@task` functions can be called the way user code calls them.

    A decorated task finds its context the same way at runtime — through the contextvar the
    Execution sets — so tests that go through the decorator exercise the real lookup.

    No teardown: the context is entered from inside the test coroutine, and asyncio runs
    every task in its own copy of the context, so the value cannot outlive the test.
    """

    def enter(context: Context) -> Context:
        set_current_context(context)
        return context

    return enter
