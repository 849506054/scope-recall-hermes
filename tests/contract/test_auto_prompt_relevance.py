"""Focused automatic recall regression: prompt relevance and session continuity."""

import itertools
from dataclasses import replace

import pytest
from scope_recall.core import CoreConfig, MemoryCore
from scope_recall.core.auto_recall import automatic_recall_skip_reason

from tests.contract.test_claims import Clock, accept, capture, draft
from tests.v11_support import context, recall_request


@pytest.mark.parametrize("query", ["算正常吗", "拆分一下组成", "现在开始", "Is that normal?", "Break it down"])
def test_generic_followup_uses_session_context_and_explicit_lookup_stays_available(tmp_path, query):
    ctx = context(tmp_path / "TEST-followup")
    core = MemoryCore(CoreConfig(ctx.binding), clock=Clock())
    core.initialize()
    core.test_sequence = itertools.count(1)
    old = replace(ctx, session_id="TEST-old-session")
    capture(core, old, f"{query} unrelated card source")
    capture(core, ctx, "当前讨论 Hermes 请求 token 消耗。")
    current = capture(core, ctx, query)
    refs = (f"{current.ref}@1",)
    with core.storage.read(ctx) as tx:
        assert automatic_recall_skip_reason(query, tx=tx, context=ctx, current_source_refs=refs)
        assert automatic_recall_skip_reason("Hermes token 消耗算正常吗", tx=tx, context=ctx) is None
        assert automatic_recall_skip_reason("以前的 token 消耗是多少", tx=tx, context=ctx) is None
        assert automatic_recall_skip_reason(
            query, tx=tx, context=replace(ctx, session_id="TEST-fresh"), current_source_refs=refs
        ) is None
    result = core.recall_packet(ctx, recall_request(query=query), current_source_refs=refs)
    assert result["items"] == []
    explicit = core.recall_packet(
        ctx, recall_request(query=query), current_source_refs=refs, background_without_evidence=False
    )
    assert explicit["items"]


def test_host_generated_completion_uses_current_conversation_and_explicit_lookup_stays_available(tmp_path):
    ctx = context(tmp_path / "TEST-host-completion")
    core = MemoryCore(CoreConfig(ctx.binding), clock=Clock())
    core.initialize()
    core.test_sequence = itertools.count(1)
    text = "[ASYNC DELEGATION BATCH COMPLETE — TEST-old] api-pool completed task"
    old = capture(core, replace(ctx, session_id="TEST-old"), text, origin="host_generated")
    current = capture(core, ctx, text.replace("TEST-old", "TEST-current"), origin="host_generated")
    refs = (f"{current.ref}@1",)
    result = core.recall_packet(ctx, recall_request(query=current.event["content"]), current_source_refs=refs)
    assert result["items"] == []
    assert "auto_recall_skipped:host_generated" in result["gaps"]
    explicit = core.recall_packet(
        ctx, recall_request(query="以前 api-pool completed task", mode="history"), background_without_evidence=False
    )
    assert old.ref in {item["ref"] for item in explicit["items"]}
    with core.storage.read(ctx) as tx:
        assert automatic_recall_skip_reason(text, tx=tx, context=ctx) is None


def test_opening_brief_and_profile_require_current_prompt_relevance(tmp_path):
    ctx = context(tmp_path / "TEST-brief")
    core = MemoryCore(CoreConfig(ctx.binding), clock=Clock())
    core.initialize()
    core.test_sequence = itertools.count(1)
    source = capture(core, ctx, "fry-cards 计数词 决定采用轮。")
    card = accept(core, ctx, draft(source, subject="fry-cards", predicate="计数词", value_text="轮")).items[0]
    source = capture(core, ctx, "fry-cards 编排要求保留 reveal_header。")
    constraint = accept(
        core, ctx,
        draft(source, kind="constraint", subject="fry-cards", predicate="编排要求", value_text="保留 reveal_header",
              statement_kind="assertion"),
    ).items[0]
    source = capture(core, ctx, "Hermes token 请求基础消耗预算 决定采用 23527。")
    token = accept(
        core, ctx, draft(source, subject="Hermes token", predicate="请求基础消耗预算", value_text="23527")
    ).items[0]
    from scope_recall.core.session_brief import rebuild_scope

    with core.storage.write(ctx) as tx:
        rebuild_scope(tx, "TEST-scope", core.clock.utc_now())
    prompt = "现在一次请求基础token消耗是多少"
    current = replace(ctx, recent_messages=(prompt,))
    brief = core.session_brief(current)
    assert token.ref in {item["ref"] for item in brief["items"]}
    assert card.ref not in {item["ref"] for item in brief["items"]}
    packet = core.recall_packet(current, recall_request(query=prompt))
    assert constraint.ref not in {item["ref"] for item in packet["items"]}
    named = replace(ctx, recent_messages=("fry-cards 计数词改为什么",))
    assert card.ref in {item["ref"] for item in core.session_brief(named)["items"]}
    compact = core.session_brief(current, compact=True)
    assert card.ref in {item["ref"] for item in compact["items"]}



def test_brief_preserves_relevance_after_literal_pipe_in_valid_content(tmp_path):
    ctx = context(tmp_path / "TEST-brief-pipe")
    core = MemoryCore(CoreConfig(ctx.binding), clock=Clock())
    core.initialize()
    core.test_sequence = itertools.count(1)
    source = capture(core, ctx, "TEST report layout table | Hermes token budget 23527")
    claim = accept(
        core, ctx,
        draft(source, subject="TEST report layout table", predicate="budget",
              value_text="table | Hermes token budget 23527"),
    ).items[0]
    from scope_recall.core.session_brief import rebuild_scope

    with core.storage.write(ctx) as tx:
        rebuild_scope(tx, "TEST-scope", core.clock.utc_now())
    current = replace(ctx, recent_messages=("Hermes token budget",))
    brief = core.session_brief(current)
    assert claim.ref in {item["ref"] for item in brief["items"]}
    assert "table | Hermes token budget 23527" in brief["text"]


def test_source_attestation_ignores_invalid_refs_and_checks_next_valid_source(tmp_path):
    ctx = context(tmp_path / "TEST-notice-refs")
    core = MemoryCore(CoreConfig(ctx.binding), clock=Clock())
    core.initialize()
    core.test_sequence = itertools.count(1)
    source = capture(core, ctx, "TEST completed task", origin="host_generated")
    with core.storage.read(ctx) as tx:
        for invalid in ("broken", "event-TEST@0", "event-TEST@01", "event-TEST@no"):
            assert automatic_recall_skip_reason(
                "Hermes token budget", tx=tx, context=ctx, current_source_refs=(invalid,)
            ) is None
            assert automatic_recall_skip_reason(
                "TEST completed task", tx=tx, context=ctx,
                current_source_refs=(invalid, f"{source.ref}@1"),
            ) == "host_generated"
