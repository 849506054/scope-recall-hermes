"""Move a vector companion into another backend, page by page.

The rows are the projection of SQLite truth: this copies them and never
rebuilds them from text (that would re-embed), and it never deletes from the
source.  Ids are the writer's own, so a copy is idempotent -- a run interrupted
anywhere re-upserts the same rows into the same points -- and the state file
says which id to resume after.

Verification reads both sides back and compares identity, payload and vector
direction.  Qdrant keeps the search vector normalised while the payload keeps
the original, so a direction comparison inside a tolerance is the honest test;
a byte comparison of floats is not.
"""
from __future__ import annotations

from collections import Counter
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, Iterable, cast

from .backup import _atomic_json

#: Ids per page: one bounded read and one bounded write, either side of the copy.
BATCH = 256
#: Direction tolerance.  The store's own decode accepts 2e-5 for the same check.
TOLERANCE = 1e-5
#: Ids named in a receipt before it says "and more".
SAMPLE = 10


def _pages(values: list[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def _backend(store: Any) -> str:
    return str(getattr(store, "backend", "?"))


def _reader(store: Any) -> Callable[[list[str]], dict[str, dict]]:
    read = getattr(store, "read_records", None)
    if not callable(read):
        raise ValueError(f"vector backend {_backend(store)} cannot be read by ids")
    return cast(Callable[[list[str]], dict[str, dict]], read)


def _writer(store: Any, seconds: float) -> Callable[[list[dict]], None]:
    """The store's fenced write when it has one, with this command's budget."""
    fenced = getattr(store, "fenced_upsert_records", None)
    if not callable(fenced):
        return cast(Callable[[list[dict]], None], store.upsert_records)

    def write(rows: list[dict]) -> None:
        fenced(rows, guard=lambda: True, remaining_seconds=seconds)

    return write


def _read_state(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if type(value) is dict else {}


def _same_direction(left: Any, right: Any, *, tolerance: float) -> bool:
    if type(left) is not list or type(right) is not list or not left or len(left) != len(right):
        return False
    norm_left, norm_right = math.hypot(*left), math.hypot(*right)
    if norm_left == 0 or norm_right == 0:
        return norm_left == norm_right
    return all(abs(a / norm_left - b / norm_right) <= tolerance for a, b in zip(left, right))


def _same_row(left: dict[str, Any], right: dict[str, Any], *, tolerance: float) -> bool:
    if set(left) != set(right):
        return False
    for key, value in left.items():
        if key != "vector" and value != right[key]:
            return False
    return _same_direction(left["vector"], right["vector"], tolerance=tolerance)


def plan(source: Any, target: Any) -> dict[str, Any]:
    """Read-only: what a migration would copy, and what the target already holds."""
    _reader(source), _reader(target)
    source_ids = sorted(source.list_ids())
    target_ids = set(target.list_ids())
    missing = [item for item in source_ids if item not in target_ids]
    extra = sorted(target_ids - set(source_ids))
    return {
        "source": _backend(source),
        "target": _backend(target),
        "source_rows": len(source_ids),
        "target_rows": len(target_ids),
        "to_copy": len(missing),
        "target_only": {"count": len(extra), "sample": extra[:SAMPLE]},
    }


def run(
    source: Any,
    target: Any,
    *,
    state_path: str | Path,
    batch_size: int = BATCH,
    seconds: float | None = None,
    batch_seconds: float = 45.0,
) -> dict[str, Any]:
    """Copy every row the target lacks, resuming after the id in the state file.

    A page is written and then recorded; a run that dies between the two repeats
    that page, which is idempotent.  ``seconds`` bounds one run so a maintenance
    window can call it repeatedly and watch ``remaining`` fall.

    Each page is written through the store's fenced entry with this command's own
    budget: a companion copy is a maintenance call, and the store's default
    per-request budget is sized for a recall, not for a 2048-dimension batch.
    """
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size")
    if seconds is not None and (type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0):
        raise ValueError("seconds")
    read_source = _reader(source)
    write = _writer(target, batch_seconds)
    state_path = Path(state_path)
    state = _read_state(state_path)
    completed = state.get("completed")
    copied_before = int(state.get("copied", 0)) if type(state.get("copied")) is int else 0
    ids = sorted(source.list_ids())
    pending = ids if completed is None else [item for item in ids if item > str(completed)]
    copied = batches = 0
    started = time.monotonic()
    for page in _pages(pending, batch_size):
        if seconds is not None and time.monotonic() - started >= seconds:
            break
        rows = read_source(page)
        if rows:
            write(list(rows.values()))
        copied += len(page)
        batches += 1
        _atomic_json(state_path, {"completed": page[-1], "copied": copied_before + copied})
    return {
        "source": _backend(source),
        "target": _backend(target),
        "source_rows": len(ids),
        "resumed_after": completed,
        "copied": copied,
        "copied_total": copied_before + copied,
        "remaining": len(ids) - copied_before - copied,
        "batches": batches,
        "seconds": round(time.monotonic() - started, 3),
    }


def verify(source: Any, target: Any, *, batch_size: int = BATCH,
           tolerance: float = TOLERANCE) -> dict[str, Any]:
    """Read both sides back: id sets, per-scope counts, payloads and directions."""
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("batch_size")
    read_source, read_target = _reader(source), _reader(target)
    source_ids = sorted(source.list_ids())
    target_ids = set(target.list_ids())
    missing = [item for item in source_ids if item not in target_ids]
    extra = sorted(target_ids - set(source_ids))
    scopes: Counter[str] = Counter()
    mismatched: list[str] = []
    shared = [item for item in source_ids if item in target_ids]
    for page in _pages(shared, batch_size):
        left, right = read_source(page), read_target(page)
        for memory_id in page:
            one, other = left.get(memory_id), right.get(memory_id)
            if one is None or other is None or not _same_row(one, other, tolerance=tolerance):
                mismatched.append(memory_id)
            else:
                scopes[str(one["scope_id"])] += 1
    return {
        "source": _backend(source),
        "target": _backend(target),
        "source_rows": len(source_ids),
        "target_rows": len(target_ids),
        "missing": {"count": len(missing), "sample": missing[:SAMPLE]},
        "target_only": {"count": len(extra), "sample": extra[:SAMPLE]},
        "mismatched": {"count": len(mismatched), "sample": mismatched[:SAMPLE]},
        "scopes": dict(sorted(scopes.items())),
        "ok": not missing and not extra and not mismatched,
    }


__all__ = ["BATCH", "SAMPLE", "TOLERANCE", "plan", "run", "verify"]
