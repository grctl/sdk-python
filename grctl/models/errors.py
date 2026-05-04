class WorkflowError(Exception):
    """Workflow error."""


class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow ID does not correspond to any active run."""
