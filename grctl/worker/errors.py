class WorkflowNotRegisteredError(Exception):
    """Exception raised when a workflow type is not registered with the worker."""

    def __init__(self, workflow_type: str) -> None:
        super().__init__(f"Workflow type '{workflow_type}' is not registered with the worker.")


class WorkflowRunnerNotFoundError(Exception):
    """Exception raised when a WorkflowRunner is not found for a given workflow run ID."""

    def __init__(self, run_id: str) -> None:
        super().__init__(f"WorkflowRunner not found for WorkflowRun ID '{run_id}'.")


class WorkflowAlreadyRunningError(Exception):
    """Exception raised when attempting to start a workflow that is already running."""

    def __init__(self, run_id: str) -> None:
        super().__init__(f"WorkflowRun ID '{run_id}' is already running.")


class NextDirectiveMissingError(Exception):
    """Exception raised when a workflow handler does not return a NextDirective."""

    def __init__(self, message: str, current_state: str) -> None:
        super().__init__(
            f"Workflow handler for step '{current_state}' did not return a NextDirective. Use `ctx.next()` to continue or `ctx.next.final()` to complete the workflow. Details: {message}",  # noqa: E501
        )
