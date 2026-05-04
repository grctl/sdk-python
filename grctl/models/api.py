import msgspec


class GrctlAPIError(msgspec.Struct):
    code: int
    message: str
    detail: str = ""


class GrctlAPIResponse(msgspec.Struct):
    success: bool
    payload: msgspec.Raw = msgspec.field(default_factory=lambda: msgspec.Raw(b""))
    error: GrctlAPIError | None = None
