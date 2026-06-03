class WorkflowError(Exception):
    """Workflow error."""


class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow ID does not correspond to any active run."""


class WorkflowAlreadyRunningError(WorkflowError):
    """Raised when a workflow ID already has an active run."""


class WorkflowTypeNotRegisteredError(WorkflowError):
    """Raised when the server has no registered worker for the requested workflow type."""
