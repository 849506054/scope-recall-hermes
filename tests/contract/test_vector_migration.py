"""A companion copy is paged, resumable and verified, and never re-embeds.

The rows copied here are the projection of SQLite truth: the tool reads them
from the running companion and writes them into another backend, comparing both
sides afterwards.  These cases use the real SQLite companion as the source and
the in-memory Qdrant double as the destination.
"""
import json

import pytest

from scope_recall.contracts import InstanceBinding
from scope_recall.core.recall_policy import SPACE_ID
from scope_recall.maintenance import vector_migration
from scope_recall.vector.qdrant_config import QdrantConfig
from scope_recall.vector.qdrant_store import QdrantVectorStore
from scope_recall.vector.store import build_vector_store
from test_qdrant_store import Server


def _row(number):
    metadata = {
        "object_kind": "event",
        "object_ref": f"TEST-{number}",
        "object_revision": 1,
        "vector_id": f"TEST-vector-{number}",
        "embedding_space": SPACE_ID,
        "agent_id": "TEST-agent",
        "installation_id": "TEST-installation",
        "project_id": None,
        "branch_id": None,
        "logical_scope_id": "TEST-scope",
    }
    return {
        "id": f"p10:TEST-{number}@1:{SPACE_ID}",
        "scope_id": "TEST-scope",
        "source": "event",
        "target": json.dumps(metadata),
        "content": f"TEST content {number}",
        "summary": "",
        "updated_at": "2026-09-25T00:00:00Z",
        "vector": [float(number), 1.0],
    }


def _source(tmp_path, count=5):
    store = build_vector_store("sqlite-bruteforce", storage_dir=tmp_path / "vectors",
                               table_name="memories", dimensions=2)
    store.open()
    store.upsert_records([_row(number) for number in range(1, count + 1)])
    return store


def _target(tmp_path):
    server = Server(tmp_path / "truth")
    store = QdrantVectorStore(
        tmp_path / "vectors-qdrant", table_name="memories", dimensions=2,
        config=QdrantConfig("http://qdrant:6333"),
        binding=InstanceBinding("TEST-agent", "TEST-installation", tmp_path / "truth",
                                frozenset({"TEST-scope"}), True),
        embedding_space=SPACE_ID, transport=server,
    )
    return store, server


def test_a_copy_is_paged_verified_and_recorded(tmp_path):
    source = _source(tmp_path)
    target, server = _target(tmp_path)
    target.open()
    state = tmp_path / "migration-state.json"
    receipt = vector_migration.run(source, target, state_path=state, batch_size=2)
    assert (receipt["copied"], receipt["remaining"], receipt["batches"]) == (5, 0, 3)
    assert receipt["source"] == "sqlite-bruteforce" and receipt["target"] == "qdrant"
    assert json.loads(state.read_text(encoding="utf-8")) == {"completed": _row(5)["id"], "copied": 5}
    result = vector_migration.verify(source, target)
    assert result["ok"] is True
    assert result["source_rows"] == result["target_rows"] == 5
    assert result["scopes"] == {"TEST-scope": 5}
    assert target.count_rows() == 5
    assert len(server.collections[target.collection_name]["points"]) == 5


def test_a_second_run_resumes_after_the_recorded_id(tmp_path):
    source = _source(tmp_path)
    target, _ = _target(tmp_path)
    target.open()
    target.upsert_records([_row(1), _row(2)])  # the first page a previous run wrote
    state = tmp_path / "migration-state.json"
    state.write_text(json.dumps({"completed": _row(2)["id"], "copied": 2}), encoding="utf-8")
    receipt = vector_migration.run(source, target, state_path=state, batch_size=2)
    assert receipt["resumed_after"] == _row(2)["id"]
    assert (receipt["copied"], receipt["copied_total"], receipt["remaining"]) == (3, 5, 0)
    assert vector_migration.verify(source, target)["ok"] is True


def test_verify_names_a_row_the_target_lost(tmp_path):
    source = _source(tmp_path)
    target, _ = _target(tmp_path)
    target.open()
    vector_migration.run(source, target, state_path=tmp_path / "state.json", batch_size=2)
    target.delete_by_ids([_row(3)["id"]])
    result = vector_migration.verify(source, target)
    assert result["ok"] is False
    assert result["missing"] == {"count": 1, "sample": [_row(3)["id"]]}
    assert result["scopes"] == {"TEST-scope": 4}


def test_verify_names_a_payload_the_target_changed(tmp_path):
    source = _source(tmp_path)
    target, server = _target(tmp_path)
    target.open()
    vector_migration.run(source, target, state_path=tmp_path / "state.json", batch_size=2)
    points = server.collections[target.collection_name]["points"]
    point = next(item for item in points.values() if item["payload"]["id"] == _row(4)["id"])
    point["payload"]["content"] = "TEST tampered"
    result = vector_migration.verify(source, target)
    assert result["ok"] is False
    assert result["mismatched"] == {"count": 1, "sample": [_row(4)["id"]]}


def test_plan_reads_both_sides_without_writing(tmp_path):
    source = _source(tmp_path)
    target, server = _target(tmp_path)
    target.open()
    vector_migration.run(source, target, state_path=tmp_path / "state.json", batch_size=2)
    server.calls.clear()
    result = vector_migration.plan(source, target)
    assert (result["source_rows"], result["target_rows"], result["to_copy"]) == (5, 5, 0)
    methods = {method for method, *_ in server.calls}
    assert "PUT" not in methods and "DELETE" not in methods  # reads only, no write path
    assert target.count_rows() == 5


def test_a_backend_that_cannot_be_read_by_ids_is_refused(tmp_path):
    class Opaque:
        backend = "opaque"

    with pytest.raises(ValueError, match="cannot be read by ids"):
        vector_migration.plan(Opaque(), _target(tmp_path)[0])
