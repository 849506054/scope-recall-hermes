"""Validated remote-companion settings; secrets remain in the process environment."""
from __future__ import annotations

from dataclasses import dataclass, fields
import re
from typing import Any, Mapping

from ..runtime.validation import mapping, only_keys, strict_float
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
        strict_float("qdrant_timeout_seconds", self.timeout_seconds, minimum=0.001, maximum=45.0)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> QdrantConfig:
        raw = mapping("qdrant", raw)
        only_keys("qdrant_fields", raw, {field.name for field in fields(cls)})
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
