from grctl.workflow import WorkflowHandle


class ChildTracker:
    """Child handles started during one step.

    They are single-step-scoped: cross-step coordination uses events/callbacks, not
    in-memory futures, so any handle still open when the step returns is abandoned
    and discarded.
    """

    def __init__(self) -> None:
        self.started: list[WorkflowHandle] = []

    def add(self, handle: WorkflowHandle) -> None:
        self.started.append(handle)

    def remove(self, handle: WorkflowHandle) -> None:
        self.started.remove(handle)

    async def discard_all(self) -> None:
        for handle in self.started:
            await handle.future.discard()
        self.started.clear()
