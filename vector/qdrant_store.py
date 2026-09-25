"""Qdrant's current collection, a rebuildable projection of SQLite truth.

The injected transport has request_json's signature. Construction performs no IO;
open() is the explicit creation boundary. Every remote mutation shares the trusted
installation's durable gate, including creation and payload-index maintenance.
REST reference: https://api.qdrant.tech/api-reference/points/upsert-points
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from ..contracts import ContractError, InstanceBinding
from ..core.deadline import remaining_seconds as ambient_remaining_seconds
from . import VectorStore, VectorStoreCompatibilityError
from .qdrant_config import QdrantConfig
from .qdrant_http import QdrantHTTPError, request_json
from .qdrant_mutation import LeaseFenceRejected, QdrantMutationGate
from .store import governed_row_ids, purge_request

_INDEX_VERSION = 1
_COLUMNS = (
    "id",
    "scope_id",
    "source",
    "target",
    "content",
    "summary",
    "updated_at",
    "vector",
)
_BATCH_SIZE = 64
_BATCH_BYTES = 4 * 1024 * 1024
_MAX_POINT_BYTES = 1024 * 1024
_PAGE_SIZE = 64


def _invalid() -> ContractError:
    return ContractError("STORAGE_UNAVAILABLE", "qdrant_invalid_response")


def _check_budget(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise QdrantHTTPError("timeout")


def _text(value: Any, name: str, *, maximum: int = 512) -> str:
    if type(value) is not str or not value.strip() or len(value) > maximum:
        raise ValueError(f"invalid qdrant {name}")
    # Reject surrogate codepoints at the boundary, before a pending marker exists.
    value.encode("utf-8")
    return value


def _direction(vector: list[float]) -> list[float]:
    # Qdrant normalizes in float32; normalize first in float64 so valid tiny/large
    # inputs cannot underflow/overflow its sum of squares and poison a write.
    norm = math.hypot(*vector)
    return [value / norm for value in vector] if norm else vector


class QdrantVectorStore(VectorStore):
    """One installation/space/table generation with deterministic UUID points.

    Payload retains the complete original VectorRecord, including its vector;
    Qdrant's separate search vector may be normalized by its Cosine metric.
    This remote payload is a projection, not a local mirror or identity authority.
    """

    backend = "qdrant"

    def __init__(
        self,
        storage_dir: Path,
        *,
        table_name: str,
        dimensions: int,
        metric: str = "cosine",
        config: QdrantConfig,
        binding: InstanceBinding,
        embedding_space: str,
        transport: Callable[..., dict] = request_json,
    ) -> None:
        if not isinstance(config, QdrantConfig) or not isinstance(
            binding, InstanceBinding
        ):
            raise TypeError("QdrantConfig and trusted InstanceBinding required")
        if type(dimensions) is not int or not 1 <= dimensions <= 32768:
            raise ValueError("invalid qdrant dimensions")
        if type(metric) is not str or metric.strip().lower() != "cosine":
            raise ValueError("qdrant supports cosine only")
        _text(table_name, "table_name", maximum=240)
        _text(embedding_space, "embedding_space", maximum=128)
        if not callable(transport):
            raise TypeError("qdrant transport must be callable")
        super().__init__(
            storage_dir, table_name=table_name, dimensions=dimensions, metric=metric
        )
        self.config = config
        self.binding = binding
        self.embedding_space = embedding_space
        identity = {
            "agent_id": binding.agent_id,
            "installation_id": binding.installation_id,
            "embedding_space": embedding_space,
            "table_name": table_name,
            "dimensions": dimensions,
            "metric": self.metric,
            "index_version": _INDEX_VERSION,
        }
        digest = hashlib.sha256(
            json.dumps(
                identity, sort_keys=True, ensure_ascii=True, separators=(",", ":")
            ).encode("ascii")
        ).hexdigest()
        self.collection_name = f"{config.collection_prefix}-{digest}"
        self.lock_directory = binding.data_directory / "qdrant"
        self._path = f"/collections/{self.collection_name}"
        self._namespace = uuid5(NAMESPACE_URL, self.collection_name)
        self._transport = transport
        self._opened = False

    @property
    def mutation_gate(self) -> QdrantMutationGate:
        # Resolve filesystem paths at use time, never in the store constructor.
        return QdrantMutationGate(self.lock_directory)

    def _deadline(self, seconds: float | None = None) -> float:
        now = time.monotonic()
        if seconds is None:
            ambient = ambient_remaining_seconds(now)
            budget = self.config.timeout_seconds if ambient is None else ambient
        else:
            # Worker fencing and maintenance carry independent, explicit budgets.
            if (
                type(seconds) not in (int, float)
                or not math.isfinite(seconds)
                or seconds <= 0
            ):
                raise ValueError("positive finite qdrant budget required")
            budget = min(float(seconds), 45.0)
        deadline = now + budget
        _check_budget(deadline)
        return deadline

    def _request(
        self, method: str, path: str, body: dict | None, deadline: float
    ) -> Any:
        _check_budget(deadline)
        reply = self._transport(self.config, method, path, body, deadline=deadline)
        _check_budget(deadline)
        if (
            type(reply) is not dict
            or reply.get("status") != "ok"
            or "result" not in reply
        ):
            raise _invalid()
        return reply["result"]

    def _require_open(self) -> None:
        if not self._opened:
            raise RuntimeError("qdrant vector collection is not open")

    def _collection_names(self, deadline: float) -> set[str]:
        result = self._request("GET", "/collections", None, deadline)
        if type(result) is not dict or type(result.get("collections")) is not list:
            raise _invalid()
        names = []
        for entry in result["collections"]:
            _check_budget(deadline)
            if (
                type(entry) is not dict
                or type(entry.get("name")) is not str
                or not entry["name"]
            ):
                raise _invalid()
            names.append(entry["name"])
        if len(names) != len(set(names)):
            raise _invalid()
        return set(names)

    def _collection_info(self, deadline: float) -> dict:
        result = self._request("GET", self._path, None, deadline)
        try:
            vectors = result["config"]["params"]["vectors"]
            if (
                type(vectors) is not dict
                or type(vectors.get("size")) is not int
                or vectors["size"] != self.dimensions
                or vectors.get("distance") != "Cosine"
                or vectors.get("multivector_config") is not None
            ):
                raise ValueError
            schema = result["payload_schema"]
            if type(schema) is not dict:
                raise ValueError
            if "scope_id" in schema and (
                type(schema["scope_id"]) is not dict
                or schema["scope_id"].get("data_type") != "keyword"
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise VectorStoreCompatibilityError(
                "Qdrant collection shape/index mismatch"
            ) from None
        return result

    def is_available(self) -> bool:
        try:
            self._collection_names(self._deadline())
            return True
        except (QdrantHTTPError, ContractError):
            return False

    def open_existing(self) -> None:
        self._opened = False
        deadline = self._deadline()
        # GET by an alias is not proof of collection identity: require the physical name.
        if self.collection_name not in self._collection_names(deadline):
            raise VectorStoreCompatibilityError("Qdrant physical collection is missing")
        self._collection_info(deadline)
        self._opened = True

    def open(self) -> None:
        self._opened = False
        deadline = self._deadline()
        gate = self.mutation_gate
        with gate.locked(deadline):
            if self.collection_name not in self._collection_names(deadline):
                with gate.mutation(
                    "create_collection", self.collection_name, deadline
                ) as receipt:
                    # Collection creation is synchronous and returns boolean, unlike
                    # point/index operations. Confirm its documented result and read back.
                    created = self._request(
                        "PUT",
                        self._path + "?wait=true",
                        {
                            "vectors": {"size": self.dimensions, "distance": "Cosine"},
                            "shard_number": 1,
                            "replication_factor": 1,
                            "write_consistency_factor": 1,
                        },
                        deadline,
                    )
                    if (
                        created is not True
                        or self.collection_name not in self._collection_names(deadline)
                    ):
                        raise _invalid()
                    self._collection_info(deadline)
                    receipt.complete()
            info = self._collection_info(deadline)
            if "scope_id" not in info["payload_schema"]:
                with gate.mutation(
                    "create_index", self.collection_name, deadline
                ) as receipt:
                    self._completed(
                        "PUT",
                        self._path + "/index?wait=true",
                        {"field_name": "scope_id", "field_schema": "keyword"},
                        deadline,
                    )
                    if (
                        "scope_id"
                        not in self._collection_info(deadline)["payload_schema"]
                    ):
                        raise _invalid()
                    receipt.complete()
        self._opened = True

    def close(self) -> None:
        # request_json owns/reaps each request's connection and helper process.
        self._opened = False

    def point_id(self, memory_id: str) -> str:
        return str(uuid5(self._namespace, _text(memory_id, "id")))

    def _vector(self, vector: Any) -> list[float]:
        if type(vector) not in (list, tuple) or len(vector) != self.dimensions:
            raise ValueError("invalid qdrant vector dimensions")
        if any(
            type(value) not in (int, float)
            or not math.isfinite(value)
            or abs(value) > 3.4028234663852886e38
            for value in vector
        ):
            raise ValueError("invalid qdrant vector number")
        return [float(value) for value in vector]

    def _row(self, row: Any) -> dict:
        if not isinstance(row, Mapping) or set(row) != set(_COLUMNS):
            raise ValueError("invalid qdrant record fields")
        for name in _COLUMNS[:-1]:
            if type(row[name]) is not str or len(row[name]) > _MAX_POINT_BYTES:
                raise ValueError("invalid qdrant record text")
            row[name].encode("utf-8")
        _text(row["id"], "id")
        _text(row["scope_id"], "scope_id")
        return dict(row) | {"vector": self._vector(row["vector"])}

    def _prepare(
        self, rows: Iterable[dict[str, Any]], deadline: float
    ) -> list[list[dict]]:
        batches: list[list[dict]] = []
        batch: list[dict] = []
        size = 32
        seen = set()
        for value in rows:
            _check_budget(deadline)
            row = self._row(value)
            if row["id"] in seen:
                raise ValueError("duplicate qdrant input id")
            seen.add(row["id"])
            point = {
                "id": self.point_id(row["id"]),
                "vector": _direction(row["vector"]),
                "payload": row,
            }
            encoded_size = (
                len(
                    json.dumps(
                        point, ensure_ascii=True, allow_nan=False, separators=(",", ":")
                    )
                )
                + 1
            )
            if encoded_size > _MAX_POINT_BYTES:
                raise ValueError("qdrant record exceeds size limit")
            if batch and (
                len(batch) >= _BATCH_SIZE or size + encoded_size > _BATCH_BYTES
            ):
                batches.append(batch)
                batch, size = [], 32
            batch.append(point)
            size += encoded_size
        if batch:
            batches.append(batch)
        _check_budget(deadline)
        return batches

    def _completed(self, method: str, path: str, body: dict, deadline: float) -> None:
        result = self._request(method, path, body, deadline)
        if type(result) is not dict or result.get("status") != "completed":
            raise ContractError("STORAGE_UNAVAILABLE", "qdrant_mutation_uncertain")

    def _decode(self, point: Any) -> dict:
        try:
            if type(point) is not dict:
                raise ValueError
            row = self._row(point["payload"])
            if type(point["id"]) is not str or point["id"] != self.point_id(row["id"]):
                raise ValueError
            actual = self._vector(point["vector"])
            expected = _direction(row["vector"])
            if any(
                not math.isclose(left, right, rel_tol=2e-5, abs_tol=2e-6)
                for left, right in zip(actual, expected)
            ):
                raise ValueError
            return row
        except (KeyError, TypeError, ValueError, OverflowError):
            raise _invalid() from None

    def _retrieve(self, ids: list[str], deadline: float) -> dict[str, dict]:
        output: dict[str, dict] = {}
        for offset in range(0, len(ids), _BATCH_SIZE):
            batch = ids[offset : offset + _BATCH_SIZE]
            result = self._request(
                "POST",
                self._path + "/points",
                {
                    "ids": [self.point_id(item) for item in batch],
                    "with_payload": True,
                    "with_vector": True,
                },
                deadline,
            )
            if type(result) is not list or len(result) > len(batch):
                raise _invalid()
            allowed = set(batch)
            for point in result:
                _check_budget(deadline)
                row = self._decode(point)
                if row["id"] not in allowed or row["id"] in output:
                    raise _invalid()
                output[row["id"]] = row
        _check_budget(deadline)
        return output

    def _upsert(
        self,
        batches: list[list[dict]],
        deadline: float,
        guard: Callable[[], bool] | None = None,
    ) -> None:
        with self.mutation_gate.mutation(
            "upsert", self.collection_name, deadline, guard=guard
        ) as receipt:
            # ponytail: bounded batches share one fence; a partial remote commit
            # leaves the whole mutation pending, requiring controlled recovery.
            for batch in batches:
                self._completed(
                    "PUT", self._path + "/points?wait=true", {"points": batch}, deadline
                )
                expected = {point["payload"]["id"]: point["payload"] for point in batch}
                if self._retrieve(list(expected), deadline) != expected:
                    raise _invalid()
            receipt.complete()

    def upsert_records(self, rows: Iterable[dict[str, Any]]) -> None:
        self._require_open()
        deadline = self._deadline()
        batches = self._prepare(rows, deadline)
        if batches:
            self._upsert(batches, deadline)

    def fenced_upsert_records(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        guard: Callable[[], bool],
        remaining_seconds: float,
    ) -> bool:
        self._require_open()
        if not callable(guard):
            raise TypeError("guard must be callable")
        deadline = self._deadline(remaining_seconds)
        batches = self._prepare(rows, deadline)
        if not batches:
            raise ValueError("fenced qdrant batch must not be empty")
        try:
            self._upsert(batches, deadline, guard)
        except LeaseFenceRejected:
            return False
        return True

    def _ids(self, ids: Iterable[str], deadline: float) -> list[str]:
        if isinstance(ids, (str, bytes)):
            raise TypeError("qdrant ids must be an iterable of identifiers")
        output = {}
        for item in ids:
            _check_budget(deadline)
            output[_text(item, "id")] = None
        return list(output)

    def _delete_batches(self, ids: list[str], deadline: float) -> None:
        for offset in range(0, len(ids), _BATCH_SIZE):
            batch = ids[offset : offset + _BATCH_SIZE]
            self._completed(
                "POST",
                self._path + "/points/delete?wait=true",
                {"points": [self.point_id(item) for item in batch]},
                deadline,
            )
        if self._retrieve(ids, deadline):
            raise _invalid()

    def delete_by_ids(self, ids: list[str]) -> None:
        self._require_open()
        deadline = self._deadline()
        listed = self._ids(ids, deadline)
        if listed:
            with self.mutation_gate.mutation(
                "delete", self.collection_name, deadline
            ) as receipt:
                self._delete_batches(listed, deadline)
                receipt.complete()

    def delete(self, ids: list[str]) -> int:
        self._require_open()
        deadline = self._deadline()
        listed = self._ids(ids, deadline)
        if not listed:
            return 0
        gate = self.mutation_gate
        with gate.locked(deadline):
            existing = self._retrieve(listed, deadline)
            with gate.mutation("delete", self.collection_name, deadline) as receipt:
                self._delete_batches(listed, deadline)
                receipt.complete()
            return len(existing)

    def contains_id(self, memory_id: str) -> bool:
        self._require_open()
        deadline = self._deadline()
        if memory_id == "":
            return False
        return bool(self._retrieve([_text(memory_id, "id")], deadline))

    def _inventory(self, deadline: float) -> dict[str, dict]:
        output: dict[str, dict] = {}
        cursor = None
        previous_point = None
        while True:
            body = {"limit": _PAGE_SIZE, "with_payload": True, "with_vector": True}
            if cursor is not None:
                body["offset"] = cursor
            result = self._request(
                "POST", self._path + "/points/scroll", body, deadline
            )
            if (
                type(result) is not dict
                or type(result.get("points")) is not list
                or "next_page_offset" not in result
                or len(result["points"]) > _PAGE_SIZE
            ):
                raise _invalid()
            for point in result["points"]:
                _check_budget(deadline)
                row = self._decode(point)
                point_id = point["id"]
                if (
                    row["id"] in output
                    or (previous_point is not None and point_id <= previous_point)
                    or (cursor is not None and point_id < cursor)
                ):
                    raise _invalid()
                output[row["id"]] = row
                previous_point = point_id
            next_cursor = result["next_page_offset"]
            if next_cursor is None:
                if self._count(deadline) != len(output):
                    raise _invalid()
                _check_budget(deadline)
                return output
            try:
                valid_cursor = (
                    type(next_cursor) is str and str(UUID(next_cursor)) == next_cursor
                )
            except ValueError:
                valid_cursor = False
            if (
                not valid_cursor
                or not result["points"]
                or previous_point is None
                or next_cursor <= previous_point
                or (cursor is not None and next_cursor <= cursor)
            ):
                raise _invalid()
            cursor = next_cursor

    def list_records(self) -> dict[str, dict[str, Any]]:
        self._require_open()
        return self._inventory(self._deadline())

    def list_ids(self) -> list[str]:
        return sorted(self.list_records())

    def _count(self, deadline: float) -> int:
        result = self._request(
            "POST", self._path + "/points/count", {"exact": True}, deadline
        )
        if (
            type(result) is not dict
            or type(result.get("count")) is not int
            or result["count"] < 0
        ):
            raise _invalid()
        return result["count"]

    def count_rows(self) -> int:
        self._require_open()
        return self._count(self._deadline())

    def audit_counts(self) -> dict[str, int]:
        self._require_open()
        deadline = self._deadline()
        # A single installation lock makes inventory/count stable against all
        # cooperating writers. Malformed UUID/payload identity is never counted away.
        with self.mutation_gate.locked(deadline):
            before = self._count(deadline)
            rows = self._inventory(deadline)
            if len(rows) != before or self._count(deadline) != before:
                raise _invalid()
            return {
                "physical_rows": before,
                "unique_ids": before,
                "duplicate_rows": 0,
                "duplicate_ids": 0,
            }

    def search(
        self, vector: list[float], *, scope_id: str, limit: int
    ) -> list[dict[str, Any]]:
        return self.search_scopes(vector, scope_ids=[scope_id], limit=limit)

    def search_scopes(
        self, vector: list[float], *, scope_ids: Iterable[str], limit: int
    ) -> list[dict[str, Any]]:
        self._require_open()
        deadline = self._deadline()
        if type(limit) is not int or limit < 0:
            raise ValueError("invalid qdrant search limit")
        scopes = self._ids(scope_ids, deadline)
        if not scopes or not vector or limit == 0:
            return []
        values = _direction(self._vector(vector))
        result = self._request(
            "POST",
            self._path + "/points/query",
            {
                "query": values,
                "filter": {"must": [{"key": "scope_id", "match": {"any": scopes}}]},
                "limit": limit,
                "with_payload": True,
                "with_vector": True,
            },
            deadline,
        )
        if (
            type(result) is not dict
            or type(result.get("points")) is not list
            or len(result["points"]) > limit
        ):
            raise _invalid()
        output = []
        seen = set()
        for point in result["points"]:
            _check_budget(deadline)
            row = self._decode(point)
            score = point.get("score")
            if (
                row["id"] in seen
                or row["scope_id"] not in scopes
                or type(score) not in (int, float)
                or not math.isfinite(score)
                or not -1.00001 <= score <= 1.00001
            ):
                raise _invalid()
            seen.add(row["id"])
            output.append(row | {"_distance": 1.0 - score})
        output.sort(key=lambda row: (row["_distance"], row["id"]))
        _check_budget(deadline)
        return output

    def purge_governed_members(
        self,
        *,
        members,
        agent_id,
        installation_id,
        partitions,
        project_id,
        branch_id,
        budget_seconds: float | None = None,
        remaining_seconds: float | None = None,
    ) -> bool:
        self._require_open()
        budget = budget_seconds if budget_seconds is not None else remaining_seconds
        if budget is None:
            raise ValueError("positive finite purge budget required")
        deadline = self._deadline(budget)
        if (agent_id, installation_id) != (
            self.binding.agent_id,
            self.binding.installation_id,
        ):
            raise ValueError("purge identity differs from trusted binding")
        for value in (project_id, branch_id):
            if value is not None:
                _text(value, "purge context", maximum=240)
        if not members or not partitions:
            return False
        targets, governed = purge_request(
            members=members,
            agent_id=agent_id,
            installation_id=installation_id,
            partitions=partitions,
        )
        for (scope, space), physical in governed.items():
            _text(scope, "purge scope", maximum=240)
            _text(space, "purge space", maximum=128)
            _text(physical, "physical scope")
        if len(governed) != len(partitions):
            raise ValueError("duplicate qdrant purge partitions")
        _check_budget(deadline)

        def inventory() -> list[str] | None:
            # Scan this whole collection: a scope filter would hide corrupt metadata
            # and turn an unclassifiable inventory into a false successful purge.
            return governed_row_ids(
                self._inventory(deadline).values(),
                targets=targets,
                governed=governed,
                agent_id=agent_id,
                installation_id=installation_id,
                project_id=project_id,
                branch_id=branch_id,
                check_budget=lambda: _check_budget(deadline),
            )

        gate = self.mutation_gate
        with gate.locked(deadline):
            ids = inventory()
            if ids is None:
                return False
            if not ids:
                return inventory() == []
            with gate.mutation("purge", self.collection_name, deadline) as receipt:
                self._delete_batches(ids, deadline)
                if inventory() != []:
                    raise _invalid()
                receipt.complete()
        return True


__all__ = ["QdrantVectorStore"]
