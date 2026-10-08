"""The Hermes adapter's automatic recall for a turn (Hermes' ``prefetch``): the turn's state read under the adapter's
lock, the recall run without it, its context rendered for the model."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import TYPE_CHECKING

from scope_recall.contracts import RecallPacket, RecallRequest
from scope_recall.core.retrieval import AUTOMATIC_PACKET_BUDGET_UNITS

from ..runtime_wiring import render_host_recall_context
from .gating import is_trivial_prompt
from .tool_surface import display_zone

if TYPE_CHECKING:
    from .provider import ScopeRecallHermesAdapter

#: Keep the adapter's log name across the module split for host logging configurations and loss counts.
_log = logging.getLogger("scope_recall.adapters.hermes.provider")

#: Seconds since a recall after which session binding warms the query route.
_QUERY_ROUTE_WARM_AFTER_SECONDS = 60.0
#: The warm-up has its own budget, independent of the recall.
_QUERY_ROUTE_WARM_BUDGET_SECONDS = 8.0

#: How long a prefetch waits for its session's state.  Hermes gives the whole prefetch 8 s and goes on without it,
#: and an automatic recall takes up to 5.
_PREFETCH_STATE_WAIT_S = 2.0


class Prefetch:
    """A Hermes adapter's turn recall; one per adapter."""

    def __init__(self, adapter: ScopeRecallHermesAdapter) -> None:
        self._adapter = adapter
        #: The last completed recall, retained across session bindings for ``warm_query_route``.
        self.last_prefetch_at = 0.0

    def recall(self, query: str, *, session_id: str = "") -> str:
        """Read the turn's state under the lock, recall without it.

        Hermes gives a prefetch 8 s and goes on with the turn while the call keeps running.  Held through the recall,
        the lock would keep the turn's tool hooks waiting behind it past Hermes' 30 s hook timeout (a prefetch that
        timed out can run 30-50 s).  Nothing of the turn is written meanwhile: its message was stored before, its
        tools run after.
        """
        if not self._adapter._lock.acquire(timeout=_PREFETCH_STATE_WAIT_S):
            self._adapter._calls.busy("prefetch")
            return ""
        try:
            identity = self._adapter._require_identity()
            effective_session = self._adapter._effective_session_id(session_id)
            if is_trivial_prompt(query):
                return ""
            if not identity.runtime_audience.allowed_scope_ids:
                self._adapter._diagnostics.capability_gaps = identity.runtime_audience.capability_gaps
                return ""
            if self._adapter._current_source_refs_overflow:
                # The overflow already reported its gap; an unfenced recall could
                # inject this turn's own sources back as memory.
                return ""
            recent = (self._adapter._current_task_message,) if self._adapter._current_task_message else ()
            context = identity.trusted_context(session_id=effective_session, recent_messages=recent)
            current_refs = tuple(self._adapter._current_source_refs)
            request = self._request(query, effective_session)
            core = self._adapter._require_core()
            turn = self._adapter._active_turn_id
        finally:
            self._adapter._lock.release()
        timed = time.monotonic()
        packet = core.recall_packet(
            context,
            request,
            current_source_refs=current_refs,
            # A day the message names is read in the zone this profile tells its model, as its memories' times are.
            zone=display_zone(),
        )
        recall_seconds = time.monotonic() - timed
        self.last_prefetch_at = time.monotonic()
        self._log_vector_loss(packet, recall_seconds)
        preparation = core.prepare_recall_render(context, packet)
        with self._adapter._lock:
            if self._adapter._active_turn_id == turn:
                # A turn begun meanwhile, after Hermes gave up on this call, keeps its own state.
                self._adapter._diagnostics.last_prefetch_request_id = packet["request_id"]
                self._adapter._diagnostics.last_render_ref = preparation.render_ref
                self._adapter._pre_llm_pending = False
        return render_host_recall_context(
            preparation.canonical_text,
            context=preparation.context,
            entry=(identity.entry_id, identity.manifest.entry_name) if identity.entry_id is not None else None,
            zone=display_zone(),
        )

    def _log_vector_loss(self, packet: RecallPacket, recall_seconds: float) -> None:
        """Record a lost semantic channel once, with the Core pipeline's failure timeline."""
        gaps = tuple(str(gap) for gap in (packet.get("gaps") or ()))
        if not any(gap == "vector_unavailable" or gap.startswith("vector_error:") for gap in gaps):
            return
        pipeline = getattr(self._adapter._require_core(), "recall_pipeline", None)
        _log.warning(
            "scope-recall: vector channel lost recall=%.2fs status=%s gaps=%s timeline=%s",
            recall_seconds,
            packet.get("status"),
            ",".join(gaps[:10]),
            json.dumps(getattr(pipeline, "last_vector_failure", None), sort_keys=True),
        )

    def warm_query_route(self) -> None:
        """Warm the query embedding route asynchronously when binding a session after an idle gap.

        A cold provider connection took 2.4-2.8 s of a 4 s recall window; an independent warm-up left the recall's
        embedding at 0.42 s.  Recent recalls keep the route warm themselves.  The thread owns its 8 s budget.
        """
        if time.monotonic() - self.last_prefetch_at < _QUERY_ROUTE_WARM_AFTER_SECONDS:
            return
        runtime = getattr(self._adapter._host_runtime, "runtime", None)
        embedder = getattr(getattr(runtime, "auxiliary", None), "query_embedding", None)
        if embedder is None:
            return

        def warm() -> None:
            try:
                embedder.embed_query("warm", remaining_seconds=_QUERY_ROUTE_WARM_BUDGET_SECONDS)
            except Exception:  # noqa: BLE001 - the warm-up owns its budget and its failure
                pass

        threading.Thread(target=warm, name="scope-recall-query-route-warmth", daemon=True).start()

    def _request(self, query: str, session_id: str) -> RecallRequest:
        self._adapter._require_identity()
        request_id = f"hermes-prefetch:{session_id}:{self._adapter._turn_counter}"
        payload: RecallRequest = {
            "protocol_version": "1.1",
            "request_id": request_id[:100],
            "query": query,
            "mode": "auto",
            "max_items": 6,
            "budget_tokens": AUTOMATIC_PACKET_BUDGET_UNITS,
        }
        return payload
