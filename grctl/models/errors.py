class WorkflowError(Exception):
    """Workflow error."""


class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow ID does not correspond to any active run."""


class WorkflowAlreadyRunningError(WorkflowError):
    """Raised when a workflow ID already has an active run."""


class WorkflowTypeNotRegisteredError(WorkflowError):
    """Raised when the server has no registered worker for the requested workflow type."""


class WorkflowStepAlreadyExecutedError(Exception):
    """Raised when a directive names a run_id that already has an execution in flight."""


ERR_WORKFLOW_ALREADY_RUNNING = 4001
ERR_WORKFLOW_RUN_NOT_FOUND = 4002
ERR_WORKFLOW_TYPE_NOT_REGISTERED = 4004

_ERROR_TYPES: dict[int, type[WorkflowError]] = {
    ERR_WORKFLOW_ALREADY_RUNNING: WorkflowAlreadyRunningError,
    ERR_WORKFLOW_RUN_NOT_FOUND: WorkflowNotFoundError,
    ERR_WORKFLOW_TYPE_NOT_REGISTERED: WorkflowTypeNotRegisteredError,
}


def error_for_code(code: int, message: str) -> WorkflowError:
    """Build the exception a server rejection code stands for.

    Codes live here rather than with the transport so every caller raises the
    same exception for the same rejection; an unrecognised code degrades to the
    base WorkflowError with the code preserved, since a newer server may reject
    for reasons this build does not know yet.
    """
    error_type = _ERROR_TYPES.get(code)
    if error_type is None:
        return WorkflowError(f"server rejected the request (code={code}): {message}")
    return error_type(message)
