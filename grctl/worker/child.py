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
    stays the raw msgpack builtins the child returned.
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
