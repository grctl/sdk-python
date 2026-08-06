"""A stable digest of a primitive value.

Replay decides whether two calls are the same call by comparing identifiers, and a
call's arguments are part of what makes it that call. Those identifiers are written
to durable history and compared against by a different process, a later build, and
possibly a machine that was not running when the run started — so the digest of a
value has to depend on the value alone, and on nothing about how or where it was
computed.
"""

import hashlib
from typing import Any

import msgspec

_DIGEST_LENGTH = 12


def fingerprint(value: Any) -> str:
    """Digest a primitive value: equal values always digest equally, in any process."""
    return hashlib.sha256(msgspec.msgpack.encode(_canonical(value))).hexdigest()[:_DIGEST_LENGTH]


def _canonical(value: Any) -> Any:
    """Order mappings by key, so a digest never depends on the order a dict was built in."""
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value
