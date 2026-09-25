"""Durable, fail-closed exclusion for Qdrant mutations.

Use one shared lock directory for every writer to a Qdrant installation, including
collection creation and purge. ``locked(deadline)`` holds inventory/empty-purge
checks without marking a write; a nested ``mutation(...)`` reuses that same lock.
The caller must acknowledge only a server-completed request whose required
postchecks passed. A crash, exception or missing acknowledgement leaves pending
state for maintenance with independent server-quiescence evidence.

ponytail: one local-node lock serializes cooperating writers; multi-node writers
need a server-side fencing protocol. Keep this directory on a trusted durable
local filesystem. Blocking filesystem syscalls cannot be preempted; deadline
checks surround them, and elapsed persistence never authorizes a request.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from typing import Callable, Iterator
from uuid import uuid4

from ..contracts import ContractError
from ..core.file_lock import advisory_file_lock

_MAX_MARKER_BYTES = 4096
_OPERATION = re.compile(r"[a-z][a-z0-9_]{0,63}\Z", re.ASCII)
_COLLECTION = re.compile(r"[A-Za-z0-9_-]{1,255}\Z", re.ASCII)
_OPERATION_ID = re.compile(r"[a-f0-9]{32}\Z", re.ASCII)
#: Refuse to follow a substituted marker path; absent on platforms without it.
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


def _uncertain() -> ContractError:
    return ContractError("STORAGE_UNAVAILABLE", "qdrant_mutation_uncertain")


def _remaining(deadline: float) -> float:
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("qdrant mutation deadline must be finite monotonic time")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("qdrant mutation deadline exhausted")
    return remaining


def _metadata(operation: str, collection: str) -> None:
    if not isinstance(operation, str) or not _OPERATION.fullmatch(operation):
        raise ValueError("qdrant mutation operation must be a bounded identifier")
    if not isinstance(collection, str) or not _COLLECTION.fullmatch(collection):
        raise ValueError("qdrant mutation collection must be a bounded identifier")


class LeaseFenceRejected(RuntimeError):
    """The locked pre-request guard returned false; nothing was issued or marked."""


class MutationReceipt:
    """Call complete() inside the mutation context after completion and postchecks."""

    def __init__(self, operation_id: str, deadline: float) -> None:
        self.operation_id = operation_id
        self._deadline = deadline
        self._completed = False
        self._closed = False

    def complete(self) -> None:
        if self._closed:
            raise RuntimeError("qdrant mutation receipt is closed")
        _remaining(self._deadline)
        self._completed = True


class QdrantMutationGate:
    """One lock and one durable pending receipt per shared installation directory.

    Construction and status() are read-only. status() returns None or validated
    pending metadata, and raises ContractError on unsafe/corrupt metadata. It is a
    diagnostic snapshot, not a substitute for holding locked() during inventory.
    """

    def __init__(self, lock_directory: str | Path) -> None:
        self.directory = Path(lock_directory).expanduser().resolve(strict=False)
        self.lock_path = self.directory / "qdrant-mutation.lock"
        self.pending_path = self.directory / "qdrant-mutation.pending.json"

    def status(self) -> dict | None:
        """Inspect only bounded regular-file metadata; never create or repair it."""
        try:
            before = self.pending_path.lstat()
        except FileNotFoundError:
            return None
        except OSError:
            raise _uncertain() from None
        try:
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or not 0 < before.st_size <= _MAX_MARKER_BYTES):
                raise _uncertain()
            flags = os.O_RDONLY | _NOFOLLOW | _NONBLOCK
            descriptor = os.open(self.pending_path, flags)
            with os.fdopen(descriptor, "rb") as handle:
                opened = os.fstat(handle.fileno())
                if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                        or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                        or not 0 < opened.st_size <= _MAX_MARKER_BYTES):
                    raise _uncertain()
                raw = handle.read(_MAX_MARKER_BYTES + 1)
            if len(raw) > _MAX_MARKER_BYTES:
                raise _uncertain()
            value = json.loads(raw)
            if (not isinstance(value, dict)
                    or set(value) != {"version", "collection", "operation_id", "operation"}
                    or type(value["version"]) is not int or value["version"] != 1
                    or not isinstance(value["operation_id"], str)
                    or not _OPERATION_ID.fullmatch(value["operation_id"])):
                raise _uncertain()
            _metadata(value["operation"], value["collection"])
            return value
        except (OSError, ValueError, RecursionError):
            raise _uncertain() from None

    def _check_clear(self) -> None:
        # Any directory entry means uncertain, including a dangling symlink.
        try:
            self.pending_path.lstat()
        except FileNotFoundError:
            return
        except OSError:
            raise _uncertain() from None
        raise _uncertain()

    @contextmanager
    def locked(self, deadline: float) -> Iterator[None]:
        """Hold shared exclusion and reject pending writes without creating one."""
        remaining = _remaining(deadline)
        with advisory_file_lock(self.lock_path, timeout_seconds=remaining):
            _remaining(deadline)
            self._check_clear()
            yield
            _remaining(deadline)

    def _sync_directory(self) -> None:
        # Windows cannot fsync a directory handle; the file fsync is the guarantee there.
        if not hasattr(os, "O_DIRECTORY"):
            return
        descriptor = os.open(self.directory, os.O_RDONLY | _DIRECTORY | _NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _persist(self, marker: dict) -> None:
        raw = json.dumps(marker, sort_keys=True, separators=(",", ":")).encode("ascii")
        if len(raw) > _MAX_MARKER_BYTES:
            raise _uncertain()
        try:
            descriptor = os.open(self.pending_path,
                                 os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise _uncertain()
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            self._sync_directory()
        except OSError:
            # Even a partially written marker blocks later mutations.
            raise _uncertain() from None

    def _clear(self, marker: dict, deadline: float) -> None:
        if self.status() != marker:
            raise _uncertain()
        _remaining(deadline)
        try:
            self.pending_path.unlink()
        except OSError:
            raise _uncertain() from None
        try:
            self._sync_directory()
            _remaining(deadline)
        except (OSError, TimeoutError):
            # A failed/late durable removal must remain fail-closed, even though
            # the server completed. Restoration has safety priority over budget.
            self._persist(marker)
            raise

    @contextmanager
    def mutation(self, operation: str, collection: str, deadline: float,
                 guard: Callable[[], bool] | None = None) -> Iterator[MutationReceipt]:
        """Mark before yielding; clear only after complete() and a clean, timely exit.

        A false guard raises LeaseFenceRejected before marking/yielding. Guard
        exceptions propagate unchanged. All phases consume the supplied absolute
        monotonic deadline. Unacknowledged clean exits raise ContractError.
        """
        _metadata(operation, collection)
        with self.locked(deadline):
            if guard is not None and not guard():
                raise LeaseFenceRejected("qdrant mutation lease fence rejected")
            _remaining(deadline)
            marker = {"version": 1, "collection": collection,
                      "operation_id": uuid4().hex, "operation": operation}
            self._persist(marker)
            _remaining(deadline)
            receipt = MutationReceipt(marker["operation_id"], deadline)
            try:
                yield receipt
                _remaining(deadline)
                if not receipt._completed:
                    raise _uncertain()
                self._clear(marker, deadline)
            finally:
                receipt._closed = True


__all__ = ["LeaseFenceRejected", "MutationReceipt", "QdrantMutationGate"]
