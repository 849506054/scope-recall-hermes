"""Current-collection REST contract, with real durable mutation gates."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import math
import threading
import time
from uuid import UUID

import pytest

from scope_recall.adapters.lance import (
    LanceIndexWriter,
    LancePurgePort,
    LanceVectorRecord,
    _record_row,
)
from scope_recall.contracts import ContractError, InstanceBinding
from scope_recall.core.deadline import RequestDeadline, using_request_deadline
from scope_recall.vector import VectorStore, VectorStoreCompatibilityError
from scope_recall.vector.qdrant_config import QdrantConfig
from scope_recall.vector.qdrant_http import QdrantHTTPError
from scope_recall.vector.qdrant_mutation import QdrantMutationGate
from scope_recall.vector.qdrant_store import QdrantVectorStore


class Server:
    """In-memory test double for documented Qdrant envelopes, never production data."""

    def __init__(self, directory):
        self.gate = QdrantMutationGate(directory / "qdrant")
        self.collections = {}
        self.calls = []
        self.hook = None
        self.page_size = 2

    def __call__(self, config, method, path, body, *, deadline):
        assert isinstance(config, QdrantConfig)
        assert deadline > time.monotonic()
        self.calls.append((method, path, deepcopy(body), deadline))
        if self.hook:
            reply = self.hook(method, path, body)
            if reply is not None:
                return reply
        route = path.split("?", 1)[0]
        parts = route.strip("/").split("/")
        if parts == ["collections"]:
            return self.ok(
                {"collections": [{"name": name} for name in self.collections]}
            )
        name = parts[1]
        if method == "PUT" and len(parts) == 2:
            assert self.gate.status()["collection"] == name
            self.collections[name] = {
                "config": {"params": deepcopy(body)},
                "payload_schema": {},
                "points": {},
            }
            return self.ok(True)
        if name not in self.collections:
            raise QdrantHTTPError("http_status", 404)
        collection = self.collections[name]
        if method == "GET" and len(parts) == 2:
            return self.ok(
                {
                    key: deepcopy(value)
                    for key, value in collection.items()
                    if key != "points"
                }
            )
        if method == "PUT" and parts[2] == "index":
            assert "wait=true" in path
            assert self.gate.status()["collection"] == name
            collection["payload_schema"][body["field_name"]] = {
                "data_type": body["field_schema"]
            }
            return self.completed()
        points = collection["points"]
        if method == "PUT" and parts[2:] == ["points"]:
            assert "wait=true" in path
            assert self.gate.status()["collection"] == name
            for point in deepcopy(body["points"]):
                norm = math.hypot(*point["vector"])
                if norm:
                    point["vector"] = [value / norm for value in point["vector"]]
                points[point["id"]] = point
            return self.completed()
        if parts[2:] == ["points", "delete"]:
            assert "wait=true" in path
            assert self.gate.status()["collection"] == name
            for point_id in body["points"]:
                points.pop(point_id, None)
            return self.completed()
        if parts[2:] == ["points", "count"]:
            assert body == {"exact": True}
            return self.ok({"count": len(points)})
        if parts[2:] == ["points"]:
            assert body["with_payload"] is True and body["with_vector"] is True
            return self.ok(
                [deepcopy(points[key]) for key in body["ids"] if key in points]
            )
        if parts[2:] == ["points", "scroll"]:
            assert "filter" not in body
            assert body["with_payload"] is True and body["with_vector"] is True
            keys = sorted(
                key
                for key in points
                if body.get("offset") is None or key >= body["offset"]
            )
            size = min(body["limit"], self.page_size)
            page = [deepcopy(points[key]) for key in keys[:size]]
            return self.ok(
                {
                    "points": page,
                    "next_page_offset": keys[size] if len(keys) > size else None,
                }
            )
        if parts[2:] == ["points", "query"]:
            scopes = body["filter"]["must"][0]["match"]["any"]
            assert body["filter"]["must"][0]["key"] == "scope_id"
            vector = body["query"]
            norm = math.hypot(*vector)
            selected = []
            for point in points.values():
                if point["payload"]["scope_id"] in scopes:
                    score = sum(
                        left * right for left, right in zip(point["vector"], vector)
                    ) / (norm or 1)
                    selected.append(deepcopy(point) | {"score": score})
            selected.sort(key=lambda point: -point["score"])
            return self.ok({"points": selected[: body["limit"]]})
        raise AssertionError((method, path, body))

    @staticmethod
    def ok(result):
        return {"status": "ok", "time": 0.001, "result": result}

    def completed(self):
        return self.ok({"operation_id": 1, "status": "completed"})


@pytest.fixture
def setup(tmp_path):
    binding = InstanceBinding(
        "agent", "install", tmp_path / "truth", frozenset({"scope"}), test_mode=True
    )
    server = Server(binding.data_directory)
    config = QdrantConfig("http://qdrant:6333", timeout_seconds=4)
    store = QdrantVectorStore(
        tmp_path / "vectors",
        table_name="memories",
        dimensions=2,
        config=config,
        binding=binding,
        embedding_space="space",
        transport=server,
    )
    return store, server, binding, config


def record(
    number=1,
    *,
    scope="scope",
    space="space",
    ref="event:one",
    agent="agent",
    installation="install",
):
    return LanceVectorRecord(
        "event",
        ref,
        number,
        f"{ref}@{number}:{space}",
        space,
        (3.0, 4.0),
        scope,
        agent,
        installation,
        updated_at="2026-09-25T00:00:00Z",
    )


def row(number=1, **kwargs):
    return _record_row(record(number, **kwargs))


def purge(store, *, space="space", ref="event:one", seconds=4):
    return LancePurgePort(
        store, embedding_spaces=[space], agent_id="agent", installation_id="install"
    ).purge_active(
        "operation",
        receipt={
            "physical_members": [{"kind": "event", "ref": ref}],
            "scope_ids": ["scope"],
        },
        remaining_seconds=seconds,
    )


def test_construction_is_io_free_and_identity_is_deterministic(
    setup, monkeypatch, tmp_path
):
    store, server, binding, config = setup
    assert isinstance(store, VectorStore)
    assert server.calls == [] and not binding.data_directory.exists()
    assert not store.db_path.exists()
    assert not QdrantVectorStore.__abstractmethods__

    def forbidden(*args, **kwargs):
        raise AssertionError("constructor performed filesystem IO")

    monkeypatch.setattr(type(tmp_path), "resolve", forbidden)
    other = QdrantVectorStore(
        tmp_path / "elsewhere",
        table_name="memories",
        dimensions=2,
        config=config,
        binding=binding,
        embedding_space="space",
        transport=server,
    )
    assert other.collection_name == store.collection_name
    assert store.collection_name.startswith(config.collection_prefix + "-")
    for kwargs in (
        {"dimensions": 3},
        {"table_name": "other"},
        {"embedding_space": "next"},
        {"binding": replace(binding, installation_id="other")},
        {"binding": replace(binding, agent_id="other")},
    ):
        values = (
            dict(
                table_name="memories",
                dimensions=2,
                config=config,
                binding=binding,
                embedding_space="space",
                transport=server,
            )
            | kwargs
        )
        assert (
            QdrantVectorStore(tmp_path, **values).collection_name
            != store.collection_name
        )


def test_open_existing_only_reads_and_shape_mismatch_is_preserved(setup):
    store, server, binding, _ = setup
    with pytest.raises((VectorStoreCompatibilityError, FileNotFoundError)):
        store.open_existing()
    assert not binding.data_directory.exists()
    store.open()
    assert server.gate.status() is None
    server.calls.clear()
    store.close()
    store.open_existing()
    assert all(method == "GET" for method, *_ in server.calls)
    server.collections[store.collection_name]["config"]["params"]["vectors"]["size"] = 3
    with pytest.raises(VectorStoreCompatibilityError):
        store.open_existing()
    assert (
        server.collections[store.collection_name]["config"]["params"]["vectors"]["size"]
        == 3
    )


def test_roundtrip_search_pagination_count_delete(setup):
    store, server, _, _ = setup
    store.open()
    rows = [row(index) for index in range(1, 6)]
    rows.append(row(6, scope="private"))
    store.upsert_records(rows)
    assert store.list_records() == {value["id"]: value for value in rows}
    assert store.list_ids() == sorted(value["id"] for value in rows)
    assert store.count_rows() == 6
    assert store.audit_counts() == {
        "physical_rows": 6,
        "unique_ids": 6,
        "duplicate_rows": 0,
        "duplicate_ids": 0,
    }
    assert store.contains_id(rows[0]["id"]) and not store.contains_id("absent")
    for key, point in server.collections[store.collection_name]["points"].items():
        assert str(UUID(key)) == key
        assert set(point["payload"]) == set(rows[0])
    server.calls.clear()
    hits = store.search_scopes(
        [3.0, 4.0],
        scope_ids=[rows[0]["scope_id"], "unused", rows[0]["scope_id"]],
        limit=3,
    )
    assert len(hits) == 3 and all(hit["_distance"] == pytest.approx(0) for hit in hits)
    queries = [call for call in server.calls if "/query" in call[1]]
    assert len(queries) == 1
    assert queries[0][2]["filter"]["must"][0]["match"]["any"] == [
        rows[0]["scope_id"],
        "unused",
    ]
    assert store.search_scopes([3.0, 4.0], scope_ids=[], limit=3) == []
    assert store.delete([rows[0]["id"], rows[0]["id"], "absent"]) == 1
    assert store.count_rows() == 5 and not store.contains_id(rows[0]["id"])
    store.close()
    with pytest.raises(RuntimeError):
        store.count_rows()


def test_fenced_adapter_checks_one_guard_and_purges_all_revisions(setup):
    store, server, _, _ = setup
    store.open()
    guards = []

    def guard():
        assert server.gate.status() is None
        guards.append(True)
        return True

    writer = LanceIndexWriter(store)
    assert writer.upsert_fenced_many(
        [record(1), record(2), record(3, ref="event:keep")],
        guard=guard,
        remaining_seconds=4,
    )
    assert guards == [True]
    assert purge(store)
    assert store.list_ids() == [record(3, ref="event:keep").vector_id]
    assert purge(store)
    assert server.gate.status() is None


def test_guard_rejection_and_bad_batch_never_mark(setup):
    store, server, _, _ = setup
    store.open()
    calls = len(server.calls)
    assert not store.fenced_upsert_records(
        [row()], guard=lambda: False, remaining_seconds=4
    )
    assert len(server.calls) == calls and server.gate.status() is None
    for bad in (
        row(2) | {"vector": [math.nan, 1]},
        row(2) | {"vector": [1]},
        row(2) | {"id": ""},
    ):
        with pytest.raises((ValueError, TypeError)):
            store.upsert_records([row(), bad])
        assert server.gate.status() is None and len(server.calls) == calls
    with pytest.raises((ValueError, TypeError)):
        store.upsert_records([row(), row()])
    assert server.gate.status() is None


@pytest.mark.parametrize(
    "failure", ["acknowledged", "wait_timeout", "timeout", "http_status", "readback"]
)
def test_uncertain_write_blocks_fresh_store_and_empty_purge(setup, failure):
    store, server, binding, config = setup
    store.open()

    def hook(method, path, body):
        if method == "PUT" and "/points" in path:
            if failure in {"timeout", "http_status"}:
                raise QdrantHTTPError(
                    failure, 503 if failure == "http_status" else None
                )
            if failure != "readback":
                return server.ok({"operation_id": 1, "status": failure})
        if (
            failure == "readback"
            and method == "POST"
            and path.split("?")[0].endswith("/points")
        ):
            return server.ok([])
        return None

    server.hook = hook
    with pytest.raises((ContractError, QdrantHTTPError, RuntimeError)):
        store.fenced_upsert_records([row()], guard=lambda: True, remaining_seconds=4)
    pending = server.gate.status()
    assert pending is not None
    server.hook = None
    other = QdrantVectorStore(
        store.db_path,
        table_name="memories",
        dimensions=2,
        config=config,
        binding=binding,
        embedding_space="space",
        transport=server,
    )
    other.open_existing()
    with pytest.raises(ContractError):
        other.upsert_records([row(2)])
    assert not purge(other)
    assert server.gate.status() == pending


@pytest.mark.parametrize(
    "damage", ["payload", "identity", "duplicate", "cursor", "missing_cursor", "vector"]
)
def test_corrupt_inventory_fails_closed(setup, damage):
    store, server, _, _ = setup
    store.open()
    store.upsert_records([row()])
    points = server.collections[store.collection_name]["points"]
    point = deepcopy(next(iter(points.values())))
    if damage == "payload":
        del point["payload"]["target"]
    elif damage == "identity":
        point["payload"]["id"] = "different"
    elif damage == "vector":
        point["vector"] = [1, 0]

    def hook(method, path, body):
        if "/scroll" in path:
            result = {
                "points": [point, point] if damage == "duplicate" else [point],
                "next_page_offset": None,
            }
            if damage == "cursor":
                result["next_page_offset"] = "not-a-uuid"
            if damage == "missing_cursor":
                del result["next_page_offset"]
            return server.ok(result)
        return None

    server.hook = hook
    with pytest.raises((ContractError, RuntimeError, ValueError)):
        store.list_records()
    with pytest.raises((ContractError, RuntimeError, ValueError)):
        store.audit_counts()
    assert not purge(store)
    assert server.gate.status() is None


def test_purge_does_not_filter_out_unclassifiable_unrelated_metadata(setup):
    store, server, _, _ = setup
    store.open()
    store.upsert_records([row(), row(2, scope="unrelated") | {"target": "broken-json"}])
    assert not purge(store)
    assert store.count_rows() == 2
    assert server.gate.status() is None


def test_ambient_deadline_is_total_across_pages(setup):
    store, server, _, _ = setup
    store.open()
    store.upsert_records([row(index) for index in range(1, 8)])
    server.calls.clear()

    def delay(method, path, body):
        if "/scroll" in path:
            time.sleep(0.03)
        return None

    server.hook = delay
    started = time.monotonic()
    with using_request_deadline(RequestDeadline.from_budget(0.05)):
        with pytest.raises((ContractError, QdrantHTTPError, TimeoutError)):
            store.list_records()
    deadlines = {call[3] for call in server.calls}
    assert len(deadlines) == 1
    assert time.monotonic() - started < 0.3
    assert server.gate.status() is None


def test_empty_purge_waits_for_writer_and_observes_its_commit(setup):
    store, server, _, _ = setup
    store.open()
    entered, release = threading.Event(), threading.Event()
    outcomes = []

    def hook(method, path, body):
        if method == "PUT" and "/points" in path:
            entered.set()
            assert release.wait(2)
        return None

    def write():
        try:
            store.upsert_records([row()])
        except Exception as error:
            outcomes.append(error)

    server.hook = hook
    writer = threading.Thread(target=write)
    deleter = threading.Thread(target=lambda: outcomes.append(purge(store)))
    writer.start()
    assert entered.wait(2)
    deleter.start()
    time.sleep(0.03)
    assert deleter.is_alive()
    release.set()
    writer.join(2)
    deleter.join(2)
    assert not writer.is_alive() and not deleter.is_alive()
    assert outcomes == [True]
    assert store.count_rows() == 0 and server.gate.status() is None


@pytest.mark.parametrize("operation", ["create", "index", "delete"])
def test_every_mutation_requires_completion_and_readback(setup, operation):
    store, server, _, _ = setup
    if operation == "delete":
        store.open()
        store.upsert_records([row()])

    def hook(method, path, body):
        if (
            operation == "create"
            and method == "PUT"
            and "/points" not in path
            and "/index" not in path
        ):
            return server.ok(True)  # Lying success: collection never appeared.
        if operation == "index" and method == "PUT" and "/index" in path:
            return server.completed()  # Lying success: keyword index never appeared.
        if operation == "delete" and "/points/delete" in path:
            return server.completed()  # Lying success: the point remains.
        return None

    server.hook = hook
    with pytest.raises((ContractError, VectorStoreCompatibilityError)):
        if operation == "delete":
            store.delete_by_ids([row()["id"]])
        else:
            store.open()
    assert server.gate.status() is not None


@pytest.mark.parametrize("damage", ["metric", "named", "index", "alias"])
def test_existing_incompatible_collection_is_never_replaced(setup, damage):
    store, server, _, _ = setup
    store.open()
    collection = server.collections[store.collection_name]
    if damage == "metric":
        collection["config"]["params"]["vectors"]["distance"] = "Dot"
    elif damage == "named":
        collection["config"]["params"]["vectors"] = {
            "other": {"size": 2, "distance": "Cosine"}
        }
    elif damage == "index":
        collection["payload_schema"]["scope_id"]["data_type"] = "integer"
    else:
        server.hook = lambda method, path, body: (
            server.ok({"collections": []}) if path == "/collections" else None
        )
    server.calls.clear()
    with pytest.raises(VectorStoreCompatibilityError):
        store.open_existing()
    assert all(call[0] == "GET" for call in server.calls)
    assert server.gate.status() is None


@pytest.mark.parametrize("seconds", [0, -1, float("nan"), float("inf"), True, "4"])
def test_invalid_fence_budget_is_rejected_without_marker(setup, seconds):
    store, server, _, _ = setup
    store.open()
    server.calls.clear()
    with pytest.raises(ValueError):
        store.fenced_upsert_records(
            [row()], guard=lambda: True, remaining_seconds=seconds
        )
    assert not server.calls and server.gate.status() is None


def test_explicit_fence_budget_is_independent_and_capped(setup):
    store, server, _, _ = setup
    store.open()
    server.calls.clear()
    started = time.monotonic()
    with using_request_deadline(RequestDeadline.from_budget(-1)):
        assert store.fenced_upsert_records(
            [row()], guard=lambda: True, remaining_seconds=90
        )
    assert all(started + 44 < call[3] <= time.monotonic() + 45 for call in server.calls)
    assert purge(store, seconds=90)


@pytest.mark.parametrize("fail_second", [False, True])
def test_split_batch_uses_one_guard_and_one_pending_receipt(
    setup, monkeypatch, fail_second
):
    from scope_recall.vector import qdrant_store as module

    monkeypatch.setattr(module, "_BATCH_SIZE", 2)
    store, server, _, _ = setup
    store.open()
    guards, markers = [], []

    def hook(method, path, body):
        if method == "PUT" and "/points" in path:
            markers.append(server.gate.status()["operation_id"])
            if fail_second and len(markers) == 2:
                raise QdrantHTTPError("timeout")
        return None

    def guard():
        guards.append(True)
        return True

    server.hook = hook
    if fail_second:
        with pytest.raises(QdrantHTTPError):
            store.fenced_upsert_records(
                [row(index) for index in range(1, 6)], guard=guard, remaining_seconds=4
            )
        assert server.gate.status()["operation_id"] == markers[0]
        assert store.count_rows() == 2
        assert not purge(store)
    else:
        assert store.fenced_upsert_records(
            [row(index) for index in range(1, 6)], guard=guard, remaining_seconds=4
        )
        assert len(markers) == 3 and store.count_rows() == 5
        assert server.gate.status() is None
        store.upsert_records([row(index) for index in range(1, 6)])
        assert store.count_rows() == 5
    assert guards == [True] and len(set(markers)) == (2 if not fail_second else 1)


def test_shared_gate_blocks_other_embedding_space_but_store_purge_is_local(setup):
    store, server, binding, config = setup
    store.open()
    other = QdrantVectorStore(
        store.db_path / "another",
        table_name="memories",
        dimensions=2,
        config=config,
        binding=binding,
        embedding_space="next",
        transport=server,
    )
    other.open()
    store.upsert_records([row()])
    other.upsert_records([row(space="next")])
    assert store.mutation_gate.directory == other.mutation_gate.directory
    assert purge(store) and other.count_rows() == 1

    def timeout(method, path, body):
        if method == "PUT" and "/points" in path:
            raise QdrantHTTPError("timeout")
        return None

    server.hook = timeout
    with pytest.raises(QdrantHTTPError):
        store.upsert_records([row()])
    assert not purge(other, space="next")
    with pytest.raises(ContractError):
        other.delete_by_ids([row(space="next")["id"]])
    assert other.count_rows() == 1


@pytest.mark.parametrize(
    "failure", ["short_page", "cursor_loop", "bad_count", "bad_envelope"]
)
def test_truncated_or_malformed_read_cannot_claim_complete_inventory(setup, failure):
    store, server, _, _ = setup
    store.open()
    store.upsert_records([row(index) for index in range(1, 6)])
    points = sorted(
        server.collections[store.collection_name]["points"].values(),
        key=lambda point: point["id"],
    )

    def hook(method, path, body):
        if failure == "bad_count" and "/count" in path:
            return server.ok({"count": True})
        if "/scroll" in path:
            if failure == "bad_envelope":
                return {"status": "ok"}
            if failure == "short_page":
                return server.ok({"points": points[:1], "next_page_offset": None})
            if failure == "cursor_loop":
                return server.ok(
                    {"points": points[:1], "next_page_offset": points[0]["id"]}
                )
        return None

    server.hook = hook
    with pytest.raises(ContractError):
        store.list_records()
    assert not purge(store)
    assert server.gate.status() is None


def test_bad_guard_and_oversized_input_never_poison_gate(setup):
    store, server, _, _ = setup
    store.open()
    server.calls.clear()

    def broken_guard():
        raise RuntimeError("lost lease")

    with pytest.raises(RuntimeError, match="lost lease"):
        store.fenced_upsert_records([row()], guard=broken_guard, remaining_seconds=4)
    with pytest.raises(ValueError):
        store.upsert_records([row() | {"content": "x" * (1024 * 1024)}])
    with pytest.raises(ValueError):
        store.delete_by_ids([row()["id"], ""])
    assert not server.calls and server.gate.status() is None


@pytest.mark.parametrize(
    "vector", [[0.0, 0.0], [1e-30, 1e-30], [1e30, 1e30], [-3.0, 4.0]]
)
def test_original_vector_roundtrip_and_float32_safe_direction(setup, vector):
    store, server, _, _ = setup
    store.open()
    source = row() | {"vector": vector}
    store.upsert_records([source])
    assert store.list_records()[source["id"]] == source
    point = next(iter(server.collections[store.collection_name]["points"].values()))
    assert math.hypot(*point["vector"]) == pytest.approx(1 if any(vector) else 0)
    hits = store.search(vector, scope_id=source["scope_id"], limit=1)
    assert hits[0]["_distance"] == pytest.approx(0 if any(vector) else 1)


def test_purge_postdelete_corruption_leaves_pending(setup):
    store, server, _, _ = setup
    store.open()
    store.upsert_records([row()])
    deleted = []

    def hook(method, path, body):
        if "/delete" in path:
            deleted.append(True)
        if deleted and "/scroll" in path:
            return server.ok({"points": [{"id": "corrupt"}], "next_page_offset": None})
        return None

    server.hook = hook
    assert not purge(store)
    assert server.gate.status()["operation"] == "purge"


def test_purge_expired_inventory_never_acknowledges_empty(setup):
    store, server, _, _ = setup
    store.open()

    def hook(method, path, body):
        if "/scroll" in path:
            time.sleep(0.03)
        return None

    server.hook = hook
    assert not purge(store, seconds=0.01)
    assert server.gate.status() is None


def test_purge_partition_mismatch_and_untrusted_identity_fail_closed(setup):
    store, server, _, _ = setup
    store.open()
    store.upsert_records([row() | {"scope_id": "wrong-physical-partition"}])
    assert not purge(store) and store.count_rows() == 1
    with pytest.raises(ValueError):
        store.purge_governed_members(
            members=[{"kind": "event", "ref": "event:one"}],
            agent_id="other",
            installation_id="install",
            partitions=[{}],
            project_id=None,
            branch_id=None,
            remaining_seconds=4,
        )
    assert server.gate.status() is None


def test_search_rejects_out_of_partition_results(setup):
    store, server, _, _ = setup
    store.open()
    store.upsert_records([row(scope="private")])
    point = next(iter(server.collections[store.collection_name]["points"].values()))
    server.hook = lambda method, path, body: (
        server.ok({"points": [point | {"score": 1}]}) if "/query" in path else None
    )
    with pytest.raises(ContractError):
        store.search([3, 4], scope_id=row()["scope_id"], limit=1)


def test_availability_is_read_only_and_http_status_is_preserved(setup):
    store, server, binding, _ = setup
    assert store.is_available()
    assert not binding.data_directory.exists()

    def denied(method, path, body):
        raise QdrantHTTPError("http_status", 401)

    server.hook = denied
    assert not store.is_available()
    with pytest.raises(QdrantHTTPError) as error:
        store.open_existing()
    assert error.value.code == "http_status" and error.value.status == 401
    assert not binding.data_directory.exists()
