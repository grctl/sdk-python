"""Worker registration: syncing the workflow type catalog to the server.

On startup a worker reports the structural definition of every workflow it
serves. The server persists this as the source of truth. Registration runs once,
before the worker subscribes to any task subject, and fails the process fast if
it cannot complete — a worker that the server does not know about must not claim
work.
"""

import asyncio
from datetime import UTC, datetime

import msgspec
from ulid import ULID

from grctl.logging_config import get_logger
from grctl.models import (
    CmdKind,
    Command,
    GrctlAPIResponse,
    RegisterCmd,
    WorkflowTypeDef,
    command_encoder,
)
from grctl.nats.connection import Connection
from grctl.worker.errors import RegistrationError
from grctl.workflow.workflow import Workflow

logger = get_logger(__name__)

# Bounded retry: registration is fail-fast, so the ceiling is small and the
# process exits once it is reached rather than retrying forever.
REGISTRATION_MAX_ATTEMPTS: int = 5
REGISTRATION_RETRY_BASE_DELAY_SECONDS: float = 0.5
REGISTRATION_REQUEST_TIMEOUT_SECONDS: float = 5.0


def build_catalog(workflows: list[Workflow]) -> list[WorkflowTypeDef]:
    """Derive a WorkflowTypeDef for each registered workflow from its handlers."""
    return [_to_type_def(wf) for wf in workflows]


def _to_type_def(workflow: Workflow) -> WorkflowTypeDef:
    start_timeout = workflow.start_handler.timeout if workflow.start_handler else None
    start_step_timeout_ms = int(start_timeout.total_seconds() * 1000) if start_timeout is not None else 0
    return WorkflowTypeDef(
        type=workflow.workflow_type,
        start_step=workflow.start_step_name or "",
        steps=workflow.step_names,
        events=workflow.event_defs,
        queries=workflow.query_names,
        start_step_timeout_ms=start_step_timeout_ms,
    )


async def register_workflow_types(
    connection: Connection,
    worker_id: str,
    catalog: list[WorkflowTypeDef],
) -> None:
    """Sync the catalog to the server via request-reply, with bounded retry.

    Raises RegistrationError once attempts are exhausted so the caller fails
    fast before subscribing to task subjects.
    """
    subject = connection.manifest.worker_command_subject()
    cmd = Command(
        id=str(ULID()),
        kind=CmdKind.worker_register,
        timestamp=datetime.now(UTC),
        msg=RegisterCmd(worker_id=worker_id, types=catalog),
        sender_id=worker_id,
    )
    data = command_encoder(cmd)

    last_error: BaseException | None = None
    for attempt in range(1, REGISTRATION_MAX_ATTEMPTS + 1):
        last_error = await _try_register(connection, subject, data, len(catalog))
        if last_error is None:
            return

        logger.warning(
            "Registration attempt %d/%d failed: %s",
            attempt,
            REGISTRATION_MAX_ATTEMPTS,
            last_error,
        )
        if attempt < REGISTRATION_MAX_ATTEMPTS:
            await asyncio.sleep(REGISTRATION_RETRY_BASE_DELAY_SECONDS * attempt)

    raise RegistrationError(
        f"failed to register workflow types after {REGISTRATION_MAX_ATTEMPTS} attempts"
    ) from last_error


async def _try_register(
    connection: Connection,
    subject: str,
    data: bytes,
    type_count: int,
) -> BaseException | None:
    """Perform one registration attempt.

    Returns None on success, or the error to retry on. Transport failures and a
    server-side rejection are treated alike — both are retried, then surfaced.
    """
    try:
        reply = await connection.nc.request(subject, data, timeout=REGISTRATION_REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:
        return exc

    response = msgspec.msgpack.decode(reply.data, type=GrctlAPIResponse)
    if response.success:
        logger.info("Registered %d workflow type(s) with server", type_count)
        return None

    return RegistrationError(f"server rejected registration: {response.error}")
