from typing import Any

import msgspec

from grctl.models.common import ErrorDetails
from grctl.models.errors import WorkflowError
from grctl.models.run_info import RunStatus


class ChildOutcome[T](msgspec.Struct):
    """Terminal outcome of a child workflow, delivered to the parent's on_completed_step.

    A single callback handles both success and failure: check `ok` (or `status`),
    then read `result` for a completed child or `error` for a failed/cancelled one.

    Parameterize the type to have the result decoded back into the child's return type,
    e.g. `outcome: ChildOutcome[OrderResult]`. Left bare (`ChildOutcome`), the result
    stays as it was received: a plain value for a child that returned a primitive, but
    the serialiser's tagged envelope for a child that returned a registered type. A
    parent that reads the result of such a child should always parameterize.

    The server sends this once per child run, when the child reaches a terminal state.
    Its field names are the callback payload's wire keys, so they are permanent.
    """

    status: RunStatus
    result: T | None = None
    error: ErrorDetails | None = None

    @property
    def ok(self) -> bool:
        """True when the child completed successfully."""
        return self.status == RunStatus.completed

    def unwrap(self) -> Any:
        """Return the result if the child completed, else raise WorkflowError.

        Lets a parent that only cares about the happy path propagate child failures
        without inspecting status explicitly.
        """
        if self.ok:
            return self.result
        message = self.error.message if self.error else f"child workflow {self.status}"
        error_type = self.error.type if self.error else "WorkflowError"
        raise WorkflowError(f"{error_type}: {message}")
