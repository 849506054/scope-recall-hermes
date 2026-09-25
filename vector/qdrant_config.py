"""Validated remote-companion settings; secrets remain in the process environment."""
from __future__ import annotations

from dataclasses import dataclass, fields
import ipaddress
import re
from typing import Any, Mapping
from urllib.parse import urlsplit

from ..runtime.validation import mapping, only_keys, strict_float


@dataclass(frozen=True)
class QdrantConfig:
    url: str
    api_key_env: str = "SCOPE_RECALL_QDRANT_API_KEY"
    collection_prefix: str = "scope-recall"
    timeout_seconds: float = 2.0

    def __post_init__(self) -> None:
        _validate_url(self.url)
        if type(self.api_key_env) is not str or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", self.api_key_env):
            raise ValueError("qdrant_api_key_env")
        if type(self.collection_prefix) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.collection_prefix):
            raise ValueError("qdrant_collection_prefix")
        strict_float("qdrant_timeout_seconds", self.timeout_seconds, minimum=0.001, maximum=45.0)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> QdrantConfig:
        raw = mapping("qdrant", raw)
        only_keys("qdrant_fields", raw, {field.name for field in fields(cls)})
        if "url" not in raw:
            raise ValueError("qdrant_url")
        return cls(**raw)


def _validate_url(value: str) -> None:
    """TLS outside an explicitly internal address; a bare HTTP(S) origin only."""
    if (type(value) is not str or len(value) > 2048 or not value.isascii()
            or any(ord(char) <= 32 or ord(char) == 127 for char in value)
            or any(char in value for char in "?#\\%")):
        raise ValueError("qdrant_url")
    try:
        parsed = urlsplit(value)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ValueError("qdrant_url") from None
    if (parsed.scheme not in {"http", "https"} or not host or parsed.username is not None
            or parsed.password is not None or parsed.path not in {"", "/"}
            or parsed.netloc.endswith(":") or port == 0):
        raise ValueError("qdrant_url")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if len(host) > 253 or any(not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
                                  for label in host.split(".")):
            raise ValueError("qdrant_url") from None
        internal = "." not in host and not host.isdigit()
    else:
        internal = address.is_loopback or any(address in network for network in (
            ipaddress.ip_network("10.0.0.0/8"), ipaddress.ip_network("172.16.0.0/12"),
            ipaddress.ip_network("192.168.0.0/16"), ipaddress.ip_network("fc00::/7"),
        ))
    if parsed.scheme == "http" and not internal:
        raise ValueError("qdrant_url")
