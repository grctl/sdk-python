from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated

import msgspec
from msgspec import UNSET, Meta, Struct, UnsetType


class WorkerResponseStatus(StrEnum):
    accepted = "accepted"
    rejected = "rejected"


class WorkerError(Struct):
    code: Annotated[str, Meta(description="Error code")]
    message: Annotated[str, Meta(description="Error message")]
    retryable: Annotated[bool, Meta(description="Whether error is retryable")]


class WorkerResponse(Struct):
    status: WorkerResponseStatus
    worker_id: str | UnsetType = UNSET
    message: Annotated[str, Meta(description="Optional response message")] | UnsetType = UNSET
    error: WorkerError | None = None


class WorkerRegistration(Struct, omit_defaults=True):
    worker_name: str
    worker_id: str
    connection_id: str
    wf_types: list[str]
    registered_at: datetime | None = None


class WorkerRegistrationResponse(Struct, omit_defaults=True):
    ok: bool
    error: str | None = None


def worker_registration_encoder(reg: WorkerRegistration) -> bytes:
    return msgspec.msgpack.encode(reg)


def worker_registration_response_decoder(data: bytes) -> WorkerRegistrationResponse:
    return msgspec.msgpack.decode(data, type=WorkerRegistrationResponse)


def worker_response_encoder(response: WorkerResponse) -> bytes:
    """Encode worker response to msgpack.

    Args:
        response: WorkerResponse to encode

    Returns:
        Msgpack-encoded bytes

    """
    return msgspec.msgpack.encode(response)


def worker_response_decoder(data: bytes) -> WorkerResponse:
    """Decode msgpack to worker response.

    Args:
        data: Msgpack-encoded bytes

    Returns:
        Decoded WorkerResponse

    Raises:
        msgspec.DecodeError: If data is malformed

    """
    return msgspec.msgpack.decode(data, type=WorkerResponse)
