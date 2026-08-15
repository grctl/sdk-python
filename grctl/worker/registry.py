from grctl.models.command import WorkflowTypeDef
from grctl.workflow import Workflow


class WorkflowRegistry:
    """The single access point for the workflows a worker serves.

    Worker registration and subscriptions both derive from this catalog, so they
    advertise and consume the same workflow types.
    """

    def __init__(self, workflows: list[Workflow]) -> None:
        self.workflows = {wf.workflow_type: wf for wf in workflows}

    def all(self) -> list[Workflow]:
        return list(self.workflows.values())

    def types(self) -> list[str]:
        return list(self.workflows)

    def type_defs(self) -> list[WorkflowTypeDef]:
        """Structural definition of every registered workflow, for server registration."""
        return [wf.type_def() for wf in self.workflows.values()]

    def get(self, workflow_type: str) -> Workflow:
        workflow = self.workflows.get(workflow_type)
        if workflow is None:
            raise ValueError(
                f"No workflow registered for type '{workflow_type}'. Registered types: {list(self.workflows)}"
            )
        return workflow
