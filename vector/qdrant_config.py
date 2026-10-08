"""Validated remote-companion settings; secrets remain in the process environment."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

from ._qdrant_http_worker import valid_origin


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
        if type(self.timeout_seconds) not in (int, float):
            raise ValueError("qdrant_timeout_seconds")
        timeout = float(self.timeout_seconds)
        if not math.isfinite(timeout) or not 0.001 <= timeout <= 45.0:
            raise ValueError("qdrant_timeout_seconds")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> QdrantConfig:
        if not isinstance(raw, Mapping):
            raise ValueError("qdrant")
        if set(raw) - {field.name for field in fields(cls)}:
            raise ValueError("qdrant_fields")
        if "url" not in raw:
            raise ValueError("qdrant_url")
        return cls(**raw)


def _validate_url(value: str) -> None:
    """TLS outside an explicitly internal address; a bare HTTP(S) origin only.

    The dialling helper owns the rule; this layer only reports its refusals.
    """
    try:
        valid_origin(value)
    except ValueError:
        raise ValueError("qdrant_url") from None
