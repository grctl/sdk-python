"""The tagged wrapper every custom-serialised value is written inside.

Values produced by a serialiser land in durable history and are read back
months later, possibly by a newer build of the same workflow. The envelope
carries the type tag and serialiser version alongside the payload so a decode
against the wrong type, or against an older encoding, fails loudly instead of
silently producing a wrong value.
"""

from typing import Any

TAG_KEY = "$type"
VERSION_KEY = "$ver"
PAYLOAD_KEY = "$val"

_ENVELOPE_KEYS = frozenset({TAG_KEY, VERSION_KEY, PAYLOAD_KEY})


def wrap(tag: str, version: int, payload: Any) -> dict[str, Any]:
    return {TAG_KEY: tag, VERSION_KEY: version, PAYLOAD_KEY: payload}


def is_envelope(raw: Any) -> bool:
    return isinstance(raw, dict) and _ENVELOPE_KEYS.issubset(raw.keys()) and len(raw) == len(_ENVELOPE_KEYS)


def read_tag(raw: dict[str, Any]) -> str:
    return str(raw[TAG_KEY])


def read_version(raw: dict[str, Any]) -> int:
    return int(raw[VERSION_KEY])


def read_payload(raw: dict[str, Any]) -> Any:
    return raw[PAYLOAD_KEY]
