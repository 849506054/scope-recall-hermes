"""A vector backend is evidence, not authority.

The companion is rebuildable and may be stale, wrong, or replaced wholesale, so
nothing it returns is trusted on its own.  Two layers stand between a hostile
answer and a packet: the adapter's row filter, which keeps a row only for the
partition, identity and embedding space the reader asked for, and SQLite
hydration, which re-checks audience, revision and lifecycle before an object is
shown.  These cases answer with what a broken or hostile backend would send.
"""
from dataclasses import replace
import json

from scope_recall.adapters.lance import LanceVectorPort
from scope_recall.core import CoreConfig, MemoryCore
from scope_recall.core.recall_policy import RecallPolicy, SPACE_ID
from scope_recall.core.retrieval import CandidateRef, SearchContext, SearchLimits
from tests.contract.test_v11_recall_admission import _capture
from tests.v11_support import context as trusted_context
from tests.v11_support import recall_request


class Clock:
    value = 100.0

    def utc_now(self):
        return "2026-09-06T12:00:00Z"

    def monotonic(self):
        return self.value


class LyingVector:
    """A port that answers with candidates a correct backend would never send."""

    def __init__(self, refs=()):
        self.refs = list(refs)
        self.calls = []

    def search(self, context, *, limit, remaining_seconds):
        self.calls.append(remaining_seconds)
        return list(self.refs)[:limit]


class SyntheticQueryEmbedding:
    def embed_query(self, text, *, remaining_seconds):
        assert text and remaining_seconds > 0
        return [1.0, 0.0]


class RowStore:
    """A store that returns rows a correct writer would never have written."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def search(self, vector, *, scope_id, limit):
        self.calls.append(scope_id)
        return list(self.rows)[:limit]


def _row(scope_id, **metadata_changes):
    metadata = {
        "object_kind": "event",
        "object_ref": "TEST-hit",
        "object_revision": 1,
        "vector_id": "TEST-vector",
        "embedding_space": SPACE_ID,
        "agent_id": "TEST-agent",
        "installation_id": "TEST-installation",
        "project_id": None,
        "branch_id": None,
        "logical_scope_id": "TEST-scope",
    }
    metadata.update(metadata_changes)
    return {"scope_id": scope_id, "target": json.dumps(metadata), "score": 0.9}


def _search_context(tmp_path, clock):
    return SearchContext(
        query="TEST semantic query",
        mode="auto",
        as_of=None,
        focus_refs=(),
        limits=SearchLimits(),
        deadline=clock.monotonic() + 5.0,
        now="2026-09-15T00:00:00Z",
        trusted_context=trusted_context(tmp_path),
    )


def test_the_adapter_keeps_only_rows_the_reader_actually_asked_for(tmp_path):
    """A row for another partition, scope, identity, space or project is not a
    candidate, and neither is a row whose own metadata cannot be read."""
    clock = Clock()
    store = RowStore([])
    port = LanceVectorPort(store, SyntheticQueryEmbedding(), clock=clock.monotonic)
    search_context = _search_context(tmp_path, clock)
    port.search(search_context, limit=1, remaining_seconds=5.0)
    partition = store.calls[0]  # the partition this reader actually asks for
    store.rows = [
        _row(partition),
        _row("TEST-other-partition"),
        _row(partition, logical_scope_id="TEST-other"),
        _row(partition, agent_id="TEST-agent-2"),
        _row(partition, installation_id="TEST-installation-2"),
        _row(partition, embedding_space="TEST-space-2"),
        _row(partition, project_id="TEST-project-2"),
        _row(partition, branch_id="TEST-branch-2"),
        _row(partition, object_revision="1"),
        {"scope_id": partition, "target": "{not json", "score": 0.9},
    ]
    hits = port.search(search_context, limit=len(store.rows), remaining_seconds=5.0)
    assert [hit.ref for hit in hits] == ["TEST-hit"]
    assert hits[0].vector_id == "TEST-vector" and hits[0].vector_score == 0.9


def test_a_lying_backend_cannot_widen_audience_or_invent_a_source(tmp_path):
    """Candidates for a scope the caller may not read, or for a source that does
    not exist at that revision, never reach a packet."""
    clock = Clock()
    ctx = trusted_context(tmp_path / "TEST-vector-authority")
    ctx = replace(ctx, binding=replace(ctx.binding, scope_ids=frozenset({"TEST-scope", "TEST-other"})))
    private = replace(ctx, allowed_scope_ids=frozenset({"TEST-other"}))
    vectors = LyingVector()
    core = MemoryCore(CoreConfig(ctx.binding), clock=clock, vectors=vectors,
                      retrieval_policy=RecallPolicy(vector_threshold=0.5))
    core.initialize()
    readable = _capture(core, ctx, "TEST/authority/readable", "TEST测试灯塔的颜色是青绿色。")
    hidden = _capture(core, private, "TEST/authority/hidden", "TEST测试灯塔的颜色是红色。")
    vectors.refs = [
        CandidateRef("event", readable.ref, readable.revision, "vector",
                     vector_score=0.9, vector_id="TEST-vector", embedding_space=SPACE_ID),
        CandidateRef("event", hidden.ref, hidden.revision, "vector",
                     vector_score=0.95, vector_id="TEST-vector-2", embedding_space=SPACE_ID),
        CandidateRef("event", "TEST-absent", 1, "vector",
                     vector_score=0.99, vector_id="TEST-vector-3", embedding_space=SPACE_ID),
        CandidateRef("event", readable.ref, readable.revision + 1, "vector",
                     vector_score=0.99, vector_id="TEST-vector-4", embedding_space=SPACE_ID),
    ]
    packet = core.recall_packet(ctx, recall_request(query="TEST测试灯塔的颜色", mode="auto"),
                                deadline_seconds=2.0)
    refs = {item["ref"] for item in packet["items"]}
    assert readable.ref in refs
    assert hidden.ref not in refs
    assert "TEST-absent" not in refs
    assert all(item["revision"] == readable.revision
               for item in packet["items"] if item["ref"] == readable.ref)
    assert vectors.calls, "the hostile answer was actually consulted"
    assert "vector_unavailable" not in packet["gaps"]


def test_a_superseded_revision_from_the_backend_is_history_not_current(tmp_path):
    """The backend may still hold a superseded revision; SQLite decides what it
    is. In a live mode it is not evidence at all, and in a history read it is
    shown as history rather than as the fact."""
    clock = Clock()
    ctx = trusted_context(tmp_path / "TEST-vector-history")
    vectors = LyingVector()
    core = MemoryCore(CoreConfig(ctx.binding), clock=clock, vectors=vectors,
                      retrieval_policy=RecallPolicy(vector_threshold=0.5))
    core.initialize()
    first = _capture(core, ctx, "TEST/history/light", "TEST测试灯塔的颜色是青绿色。", revision=1)
    second = _capture(core, ctx, "TEST/history/light", "TEST测试灯塔的颜色是红色。", revision=2)
    assert first.ref == second.ref and first.revision == 1 and second.revision == 2
    vectors.refs = [
        CandidateRef("event", first.ref, 1, "vector",
                     vector_score=0.9, vector_id="TEST-vector-old", embedding_space=SPACE_ID),
    ]
    live = core.recall_packet(ctx, recall_request(query="TEST测试灯塔的颜色", mode="auto"),
                              deadline_seconds=2.0)
    assert all(item["revision"] != 1 for item in live["items"] if item["ref"] == first.ref)
    history = core.recall_packet(ctx, recall_request(query="TEST测试灯塔的颜色", mode="history"),
                                 deadline_seconds=2.0)
    stale = [item for item in history["items"] if item["ref"] == first.ref and item["revision"] == 1]
    assert stale and all(item["temporal_status"] == "historical" for item in stale)
