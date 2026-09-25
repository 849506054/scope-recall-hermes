"""The durable gate fails closed across crashes and shares the caller's deadline."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import threading
import time

import pytest

from scope_recall.contracts import ContractError
from scope_recall.vector import qdrant_mutation as mutation


COLLECTION = "scope-recall-test"


def future(seconds=5):
    return time.monotonic() + seconds


def assert_uncertain(error):
    assert error.value.code == "STORAGE_UNAVAILABLE"
    assert error.value.field == "qdrant_mutation_uncertain"


def child(repo, directory, action):
    # A fresh interpreter proves locking rather than inheriting a threading.RLock.
    script = """
import sys, time, types
from pathlib import Path
package = types.ModuleType('scope_recall')
package.__path__ = [sys.argv[1]]
sys.modules['scope_recall'] = package
from scope_recall.vector.qdrant_mutation import QdrantMutationGate
gate = QdrantMutationGate(Path(sys.argv[2]))
if sys.argv[3] == 'write':
    with gate.mutation('upsert', 'scope-recall-test', time.monotonic() + 30) as tx:
        print('issued', flush=True)
        sys.stdin.readline()
        tx.complete()
    print('completed', flush=True)
else:
    print('waiting', flush=True)
    with gate.locked(time.monotonic() + 30):
        print('inventory', flush=True)
        sys.stdin.readline()
    print('purged-empty', flush=True)
"""
    return subprocess.Popen(
        [sys.executable, "-u", "-c", script, str(repo), str(directory), action],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def line(process):
    # Bound failed tests too: never block the suite on a broken lock protocol.
    result = []
    thread = threading.Thread(target=lambda: result.append(process.stdout.readline()), daemon=True)
    thread.start()
    thread.join(5)
    assert not thread.is_alive(), "child failed to reach checkpoint"
    assert result[0], "child exited before checkpoint"
    return result[0].strip()


def stop(*processes):
    for process in processes:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()


def test_status_is_read_only_and_does_not_create_directory(tmp_path):
    directory = tmp_path / "missing" / "gate"
    gate = mutation.QdrantMutationGate(directory)
    assert gate.status() is None
    assert not directory.parent.exists()


def test_ack_removes_only_after_clean_exit_and_persists_minimal_marker(tmp_path, monkeypatch):
    gate = mutation.QdrantMutationGate(tmp_path / "gate")
    calls = []
    real_sync = os.fsync

    def fsync(fd):
        calls.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        real_sync(fd)

    monkeypatch.setattr(mutation.os, "fsync", fsync)
    ids = []
    for operation in ("create_collection", "upsert", "delete", "purge"):
        with gate.mutation(operation, COLLECTION, future()) as tx:
            marker = gate.status()
            assert set(marker) == {"version", "collection", "operation_id", "operation"}
            assert marker["version"] == 1
            assert marker["collection"] == COLLECTION
            assert marker["operation"] == operation
            assert marker["operation_id"] == tx.operation_id
            assert len(marker["operation_id"]) == 32
            assert calls[-2:] == ["file", "directory"]
            # Owner-only is the property; the exact bits are the filesystem's
            # (a btrfs mount with trimacl answers 0o700 for a 0o600 request).
            assert stat.S_IMODE(gate.pending_path.stat().st_mode) & 0o077 == 0
            ids.append(tx.operation_id)
            tx.complete()
            assert gate.pending_path.exists()
        assert gate.status() is None
        assert calls[-1] == "directory"
    assert len(set(ids)) == len(ids)


@pytest.mark.parametrize("ack", [False, True])
def test_exception_keeps_pending_even_after_ack(tmp_path, ack):
    gate = mutation.QdrantMutationGate(tmp_path)
    with pytest.raises(RuntimeError, match="postcheck"):
        with gate.mutation("delete", COLLECTION, future()) as tx:
            if ack:
                tx.complete()
            raise RuntimeError("postcheck")
    marker = gate.pending_path.read_bytes()
    with pytest.raises(ContractError) as error:
        with gate.mutation("upsert", "another-collection", future()):
            pytest.fail("request issued with unresolved earlier write")
    assert_uncertain(error)
    assert gate.pending_path.read_bytes() == marker


def test_missing_ack_is_not_success(tmp_path):
    gate = mutation.QdrantMutationGate(tmp_path)
    with pytest.raises(ContractError) as error:
        with gate.mutation("upsert", COLLECTION, future()):
            pass
    assert_uncertain(error)
    assert gate.status()["operation"] == "upsert"


def test_guard_rejects_before_marker_or_request(tmp_path):
    gate = mutation.QdrantMutationGate(tmp_path)
    calls = []

    def guard():
        assert gate.status() is None
        calls.append("guard")
        return False

    with pytest.raises(mutation.LeaseFenceRejected):
        with gate.mutation("upsert", COLLECTION, future(), guard=guard):
            calls.append("request")
    assert calls == ["guard"]
    assert gate.status() is None


def test_guard_exception_leaves_no_marker(tmp_path):
    gate = mutation.QdrantMutationGate(tmp_path)

    def guard():
        raise RuntimeError("lease lost")

    with pytest.raises(RuntimeError, match="lease lost"):
        with gate.mutation("upsert", COLLECTION, future(), guard=guard):
            pytest.fail("request")
    assert gate.status() is None


def test_locked_inventory_is_reentrant_with_mutation_and_empty_purge_is_unmarked(tmp_path):
    gate = mutation.QdrantMutationGate(tmp_path)
    with gate.locked(future()):
        assert gate.status() is None
        # A different object must still share the physical lock identity.
        other = mutation.QdrantMutationGate(tmp_path)
        with other.mutation("purge", COLLECTION, future()) as tx:
            tx.complete()
        with other.locked(future()):
            assert gate.status() is None
    assert gate.status() is None
    with pytest.raises(ContractError):
        with gate.mutation("delete", COLLECTION, future()):
            pass
    with pytest.raises(ContractError) as error:
        with gate.locked(future()):
            pytest.fail("empty purge crossed pending write")
    assert_uncertain(error)


def test_real_sigkill_preserves_pending_and_blocks_mutations_and_empty_purge(tmp_path):
    repo = Path(__file__).resolve().parents[2]
    process = child(repo, tmp_path, "write")
    gate = mutation.QdrantMutationGate(tmp_path)
    try:
        assert line(process) == "issued"
        before = gate.pending_path.read_bytes()
        process.kill()
        assert process.wait(timeout=5) == -signal.SIGKILL
        assert gate.pending_path.read_bytes() == before
        for context in (gate.mutation("delete", COLLECTION, future()), gate.locked(future())):
            with pytest.raises(ContractError) as error:
                with context:
                    pytest.fail("passed unresolved killed write")
            assert_uncertain(error)
    finally:
        stop(process)


def test_two_processes_serialize_empty_purge_inventory_after_write(tmp_path):
    repo = Path(__file__).resolve().parents[2]
    writer = child(repo, tmp_path, "write")
    inventory = None
    try:
        assert line(writer) == "issued"
        inventory = child(repo, tmp_path, "inventory")
        assert line(inventory) == "waiting"
        # No inventory checkpoint can arrive while the write still owns the gate.
        observed = []
        reader = threading.Thread(target=lambda: observed.append(inventory.stdout.readline()), daemon=True)
        reader.start()
        reader.join(.1)
        assert reader.is_alive()
        writer.stdin.write("complete\n")
        writer.stdin.flush()
        assert line(writer) == "completed"
        reader.join(5)
        assert observed == ["inventory\n"]
        inventory.stdin.write("release\n")
        inventory.stdin.flush()
        assert line(inventory) == "purged-empty"
        assert writer.wait(timeout=5) == inventory.wait(timeout=5) == 0
    finally:
        stop(*([writer, inventory] if inventory is not None else [writer]))


def test_cross_process_lock_wait_consumes_deadline_before_guard(tmp_path):
    process = child(Path(__file__).resolve().parents[2], tmp_path, "inventory")
    gate = mutation.QdrantMutationGate(tmp_path)
    calls = []
    try:
        assert line(process) == "waiting"
        assert line(process) == "inventory"
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            with gate.mutation("delete", COLLECTION, start + .12,
                               guard=lambda: calls.append("guard")):
                pytest.fail("request")
        assert .09 <= time.monotonic() - start < 1.0
        assert calls == []
        assert gate.status() is None
    finally:
        stop(process)


def test_cross_thread_lock_and_guard_order(tmp_path):
    gate = mutation.QdrantMutationGate(tmp_path)
    entered = threading.Event()
    release = threading.Event()

    def inventory():
        with mutation.QdrantMutationGate(tmp_path).locked(future()):
            entered.set()
            release.wait(5)

    thread = threading.Thread(target=inventory)
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(TimeoutError):
            with gate.mutation("upsert", COLLECTION, future(.08), guard=lambda: pytest.fail("guard")):
                pytest.fail("request")
        assert gate.status() is None
    finally:
        release.set()
        thread.join(5)
    with gate.mutation("upsert", COLLECTION, future(), guard=lambda: True) as tx:
        tx.complete()


@pytest.mark.parametrize("stage", ["before", "guard", "persist", "body", "after_ack"])
def test_one_deadline_for_every_stage(tmp_path, monkeypatch, stage):
    gate = mutation.QdrantMutationGate(tmp_path)
    now = [time.monotonic()]
    deadline = now[0] + 1
    monkeypatch.setattr(mutation.time, "monotonic", lambda: now[0])
    real_sync = os.fsync

    def guard():
        if stage == "guard":
            now[0] = deadline
        return True

    def fsync(fd):
        real_sync(fd)
        if stage == "persist" and stat.S_ISREG(os.fstat(fd).st_mode):
            now[0] = deadline

    monkeypatch.setattr(mutation.os, "fsync", fsync)
    if stage == "before":
        now[0] = deadline
    with pytest.raises(TimeoutError):
        with gate.mutation("upsert", COLLECTION, deadline, guard=guard) as tx:
            assert stage in {"body", "after_ack"}, "expired operation issued request"
            if stage == "after_ack":
                tx.complete()
            now[0] = deadline
            if stage == "body":
                tx.complete()
    assert (gate.status() is not None) == (stage in {"persist", "body", "after_ack"})


@pytest.mark.parametrize("kind", ["corrupt", "oversize", "symlink", "dangling", "directory", "fifo", "hardlink"])
def test_unsafe_markers_fail_closed_and_are_preserved(tmp_path, kind):
    gate = mutation.QdrantMutationGate(tmp_path)
    target = tmp_path / "target"
    target.write_text("private target data")
    if kind == "corrupt":
        gate.pending_path.write_text("{broken")
    elif kind == "oversize":
        gate.pending_path.write_bytes(b" " * 8192)
    elif kind in {"symlink", "dangling"}:
        gate.pending_path.symlink_to(target if kind == "symlink" else tmp_path / "missing")
    elif kind == "directory":
        gate.pending_path.mkdir()
    elif kind == "fifo":
        os.mkfifo(gate.pending_path)
    else:
        os.link(target, gate.pending_path)
    before = gate.pending_path.lstat()
    with pytest.raises(ContractError) as error:
        gate.status()
    assert_uncertain(error)
    for context in (gate.mutation("purge", COLLECTION, future()), gate.locked(future())):
        with pytest.raises(ContractError) as error:
            with context:
                pytest.fail("unsafe marker accepted")
        assert_uncertain(error)
    assert gate.pending_path.lstat().st_ino == before.st_ino
    assert target.read_text() == "private target data"


@pytest.mark.parametrize("bad", [None, True, float("inf"), float("nan"), "1"])
def test_invalid_deadlines_do_not_create_marker(tmp_path, bad):
    gate = mutation.QdrantMutationGate(tmp_path / "new")
    with pytest.raises(ValueError):
        with gate.mutation("upsert", COLLECTION, bad):
            pytest.fail("request")
    assert gate.status() is None


@pytest.mark.parametrize("field,value", [("operation", "payload with secret"), ("collection", "../escape"),
                                         ("operation", "x" * 5000), ("collection", "x" * 5000)])
def test_marker_metadata_is_bounded_identifiers(tmp_path, field, value):
    gate = mutation.QdrantMutationGate(tmp_path)
    args = {"operation": "upsert", "collection": COLLECTION, field: value}
    with pytest.raises(ValueError):
        with gate.mutation(**args, deadline=future()):
            pytest.fail("request")
    assert gate.status() is None


def test_marker_creation_is_exclusive_and_no_follow(tmp_path, monkeypatch):
    gate = mutation.QdrantMutationGate(tmp_path)
    opens = []
    real_open = os.open

    def opened(path, flags, *args, **kwargs):
        if Path(path).name == gate.pending_path.name:
            opens.append(flags)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(mutation.os, "open", opened)
    with gate.mutation("upsert", COLLECTION, future()) as tx:
        tx.complete()
    creation = [flags for flags in opens if flags & os.O_CREAT]
    assert len(creation) == 1
    assert creation[0] & os.O_EXCL
    assert creation[0] & os.O_NOFOLLOW


def test_failed_persistence_never_issues_request_and_keeps_marker(tmp_path, monkeypatch):
    gate = mutation.QdrantMutationGate(tmp_path)
    real_sync = os.fsync

    def fsync(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("disk sync failed")
        real_sync(fd)

    monkeypatch.setattr(mutation.os, "fsync", fsync)
    with pytest.raises(ContractError) as error:
        with gate.mutation("upsert", COLLECTION, future()):
            pytest.fail("request")
    assert_uncertain(error)
    assert gate.pending_path.exists()


def test_changed_marker_is_not_deleted_on_ack(tmp_path):
    gate = mutation.QdrantMutationGate(tmp_path)
    with pytest.raises(ContractError) as error:
        with gate.mutation("upsert", COLLECTION, future()) as tx:
            payload = json.loads(gate.pending_path.read_text())
            payload["operation_id"] = "a" * 32
            gate.pending_path.write_text(json.dumps(payload))
            tx.complete()
    assert_uncertain(error)
    assert gate.pending_path.exists()
