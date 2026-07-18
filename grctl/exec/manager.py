from typing import Protocol

from grctl.exec.execution import Execution
from grctl.models.handler import HandlerConfig
from grctl.workflow import Workflow


class KVStore(Protocol):
    def get(self, key: str) -> str: ...

    def set(self, key: str, value: str) -> None: ...


class WorkflowRegistry:
    workflows: dict[str, Workflow] = {}

    def register(self, workflow: Workflow) -> None:
        self.workflows[workflow.workflow_type] = workflow

    def get(self, workflow_type: str) -> Workflow:
        return self.workflows[workflow_type]

    def handler_config(self, wf_type: str, step_name: str) -> HandlerConfig:
        wf = self.workflows.get(wf_type)
        if wf is None:
            raise ValueError(f"Workflow type {wf_type} not found")
        # TODO: Workflow should register handlers directly on the Workflow Registery
        handler = wf._step_handlers.get(step_name)  # noqa: SLF001
        if handler is None:
            raise ValueError(f"Step {step_name} not found in workflow {wf_type}")
        return handler


class ExecutionManager:
    workflows: WorkflowRegistry

    def __init__(self, execution: Execution, kvstore: KVStore) -> None:
        self.execution = execution
        self.kvstore = kvstore
        self.workflows = WorkflowRegistry()
