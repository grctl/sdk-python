import msgspec

_NIL_PAYLOAD = msgspec.msgpack.encode(None)
"""Encoded nil, used as the empty payload.

msgspec.Raw is spliced into the output verbatim, so an empty Raw contributes no
bytes at all and produces a map whose header promises more pairs than it holds —
unparseable by any client. The default has to be a valid encoding of something.
"""


class GrctlAPIError(msgspec.Struct):
    code: int
    message: str
    detail: str = ""


class GrctlAPIResponse(msgspec.Struct):
    success: bool
    payload: msgspec.Raw = msgspec.field(default_factory=lambda: msgspec.Raw(_NIL_PAYLOAD))
    error: GrctlAPIError | None = None
