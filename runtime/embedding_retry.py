"""A second attempt at the query embedding inside the recall's own window.

Losing the query embedding costs the whole semantic channel for that recall
(reported as ``vector_unavailable``, never silent).  A request the provider
rejected fails the same way twice, so only a connection failure or a call that
timed out is asked again.

Two ways the second attempt is reached, and the first one keeps its full budget
in both:

* the first attempt fails fast (a connection that could not be opened, a
  surface the provider refused) and time remains -- it is retried at once;
* the first attempt has not answered within the hedge delay -- a second one
  starts on another connection while the first is still out, and the first
  answer wins.  A healthy request is never aborted to make room for it.

A call that hangs therefore costs the delay, not the channel.  Paid
consolidation calls have their own durable recovery in ``AUTO_RECOVERABLE_ERRORS``;
this is the read path's only retry.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from contextvars import copy_context
from queue import Empty, Queue
from typing import Any

from ..core.failure_policy import CONNECTION_FAILURES

#: A second attempt must be able to use at least this share of the original budget.
MINIMUM_RETRY_FRACTION = 0.3

#: How long the first attempt may stay silent before a second one starts beside it.
HEDGE_DELAY_SECONDS = 1.2

#: The hedge never waits longer than this share of the budget, so a short window
#: still gets its second attempt early enough to answer.
HEDGE_DELAY_SHARE = 0.45

#: Below this much time left, a second attempt cannot answer and is not started.
MINIMUM_HEDGE_SECONDS = 0.3

#: The first attempt and the one asked for beside it, and no more: a third would
#: spend the window on a route that has already failed twice.
MAXIMUM_ATTEMPTS = 2


def transient(error: BaseException) -> bool:
    """Whether this failure was the connection rather than the request."""
    return getattr(error, "error_type", None) in CONNECTION_FAILURES


def retryable(error: BaseException) -> bool:
    """Whether asking again may answer: the connection failed, or the call timed out."""
    return transient(error) or getattr(error, "error_type", None) == "timeout"


def retry_budget(budget_seconds: float, remaining_seconds: float) -> float:
    """Seconds a second attempt may use, or ``0.0`` when it must not run."""
    if type(budget_seconds) not in (int, float) or type(remaining_seconds) not in (int, float):
        # Exact types, as everywhere else a bound is read: a string that happens
        # to parse is a caller bug, and silently coercing it hides one.
        return 0.0
    budget, remaining = float(budget_seconds), float(remaining_seconds)
    if not budget > 0 or not remaining > 0 or budget != budget or remaining != remaining:
        return 0.0
    if remaining < budget * MINIMUM_RETRY_FRACTION:
        return 0.0
    return min(budget, remaining)


def embed_with_one_retry(
    embed: Callable[[float], Sequence[float]], *, budget_seconds: float, remaining: Callable[[], float]
) -> Any:
    """Run ``embed(seconds)``, retrying once if the connection was what failed.

    ``remaining`` is read again after the failure rather than passed in, so the
    second attempt is budgeted from the time the first one actually left.

    A call that hangs holds its whole budget and reaches the retry with nothing
    left; the recall path asks through :func:`embed_with_hedge` instead, which
    starts the second attempt while the first is still out.
    """
    try:
        return embed(budget_seconds)
    except Exception as error:
        if not retryable(error):
            raise
        seconds = retry_budget(budget_seconds, remaining())
        if seconds <= 0:
            raise
        return embed(seconds)


def embed_with_hedge(
    embed: Callable[[float], Sequence[float]],
    *,
    budget_seconds: float,
    remaining: Callable[[], float],
    delay_seconds: float = HEDGE_DELAY_SECONDS,
) -> Any:
    """Run ``embed`` and answer with the first attempt that comes back.

    The first attempt keeps ``budget_seconds`` whole, so a slow but healthy
    request is never aborted.  When it is still silent after the hedge delay,
    or fails in a way another attempt may answer, a second one runs beside it on
    another connection.  The context of the caller is copied into either thread,
    so the request deadline and the outbound check it carries still apply.
    """
    budget = float(budget_seconds)
    if not budget > 0:
        return embed(budget_seconds)
    delay = max(0.0, min(delay_seconds, budget * HEDGE_DELAY_SHARE))
    outcomes: Queue[tuple[bool, Any]] = Queue()
    context = copy_context()

    def attempt(seconds: float) -> None:
        def run() -> None:
            try:
                outcomes.put((True, embed(seconds)))
            except BaseException as exc:  # noqa: BLE001 - handed to the caller below
                outcomes.put((False, exc))

        threading.Thread(
            target=context.copy().run,
            args=(run,),
            name="scope-recall-query-embedding",
            daemon=True,
        ).start()

    attempt(budget_seconds)
    attempts = 1
    outstanding = 1
    wait = delay
    last: BaseException | None = None
    while outstanding:
        left = remaining()
        if left <= 0:
            break
        try:
            ok, value = outcomes.get(timeout=min(wait, left))
        except Empty:
            left = remaining()
            seconds = (
                0.0
                if (last is not None or outstanding > 1 or attempts >= MAXIMUM_ATTEMPTS)
                else retry_budget(budget, left)
            )
            if seconds > MINIMUM_HEDGE_SECONDS:
                attempt(seconds)
                attempts += 1
                outstanding += 1
                wait = left
                continue
            break
        outstanding -= 1
        if ok:
            return value
        last = value
        if not retryable(value):
            raise value
        left = remaining()
        if outstanding:
            wait = left
            continue
        if attempts >= MAXIMUM_ATTEMPTS:
            raise value
        seconds = retry_budget(budget, left)
        if seconds <= 0:
            raise value
        attempt(seconds)
        attempts += 1
        outstanding += 1
        wait = left
    if last is not None:
        raise last
    from .models import AuxiliaryModelError

    raise AuxiliaryModelError("timeout", phase="prefetch")


__all__ = [
    "HEDGE_DELAY_SECONDS",
    "HEDGE_DELAY_SHARE",
    "MINIMUM_HEDGE_SECONDS",
    "MINIMUM_RETRY_FRACTION",
    "embed_with_hedge",
    "embed_with_one_retry",
    "retry_budget",
    "retryable",
    "transient",
]
