"""One-shot, deadline-bounded Qdrant JSON requests with a private stdlib helper.

Each request owns one process; write failures are uncertain commits, never retried.
Only stdin carries the API key. Errors expose a closed code and optional HTTP status.
"""
from __future__ import annotations

import base64
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

from . import _qdrant_http_worker as wire
from .qdrant_config import QdrantConfig

_WORKER_PATH = Path(__file__).with_name("_qdrant_http_worker.py")
_CLEANUP_GRACE = 0.2


class QdrantHTTPError(Exception):
    """Sanitized failure, safe to propagate into diagnostics and store decisions."""

    def __init__(self, code: str, status: int | None = None):
        self.code = code if type(code) is str and code in wire.ERROR_CODES else "worker_protocol"
        self.status = status if type(status) is int and 100 <= status <= 599 else None
        super().__init__(self.code)


def _worker_reply(raw: bytes) -> dict:
    if len(raw) > wire.MAX_RESPONSE_FRAME:
        raise QdrantHTTPError("worker_protocol")
    try:
        frame = wire._load_object(raw)
        status = frame.get("status")
        if status is not None and (type(status) is not int or not 100 <= status <= 599):
            raise ValueError
        if frame.get("ok") is False:
            if (set(frame) != {"ok", "code", "status"} or type(frame["code"]) is not str
                    or frame["code"] not in wire.ERROR_CODES):
                raise ValueError
            raise QdrantHTTPError(frame["code"], status=status)
        if (frame.get("ok") is not True or set(frame) != {"ok", "status", "body_b64"}
                or status is None or not 200 <= status < 300 or type(frame["body_b64"]) is not str):
            raise ValueError
        body = base64.b64decode(frame["body_b64"], validate=True)
        if len(body) > wire.MAX_RESPONSE_BYTES:
            raise ValueError
        return wire._load_object(body)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise QdrantHTTPError("worker_protocol") from None


def _finish_process(process, thread):
    """Kill only the owned helper, then reap it and let its I/O owner close pipes."""
    cleanup_deadline = time.monotonic() + _CLEANUP_GRACE
    try:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=max(0, cleanup_deadline - time.monotonic()))
    except (OSError, subprocess.TimeoutExpired):
        # A delayed OS reap must not extend the caller's budget indefinitely.
        def reap():
            try:
                process.wait()
            except OSError:
                pass
        threading.Thread(target=reap, name="scope-recall-qdrant-reap", daemon=True).start()
    if thread is not None:
        thread.join(timeout=max(0, cleanup_deadline - time.monotonic()))
    else:
        for stream in (process.stdin, process.stdout):
            if stream is not None:
                stream.close()


def _exchange(request: bytes, *, deadline: float) -> dict:
    process = None
    thread = None
    done = threading.Event()
    # Slot 0 always holds the outcome, so a failed I/O thread can never leave it empty.
    outcome: list[dict | QdrantHTTPError] = [QdrantHTTPError("worker_protocol")]
    try:
        wire._remaining(deadline)
        # The exact interpreter path preserves venv identity; -I ignores ambient Python paths.
        environment = ({"SystemRoot": os.environ["SystemRoot"]}
                       if os.name == "nt" and "SystemRoot" in os.environ else {})
        process = subprocess.Popen(
            [sys.executable, "-I", "-B", str(_WORKER_PATH)], env=environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
            close_fds=True,
        )

        def exchange():
            try:
                with process.stdin, process.stdout:
                    process.stdin.write(request)
                    process.stdin.close()
                    # A runaway helper cannot allocate an unbounded communicate() buffer.
                    raw = process.stdout.read(wire.MAX_RESPONSE_FRAME + 1)
                    outcome[0] = _worker_reply(raw)
            except QdrantHTTPError as error:
                outcome[0] = error
            except Exception:
                outcome[0] = QdrantHTTPError("worker_protocol")
            finally:
                done.set()

        thread = threading.Thread(target=exchange, name="scope-recall-qdrant-io", daemon=True)
        thread.start()
        if not done.wait(timeout=wire._remaining(deadline)):
            raise QdrantHTTPError("timeout")
        wire._remaining(deadline)
        value = outcome[0]
        failure = value if isinstance(value, QdrantHTTPError) else None
        if failure is not None and failure.code == "worker_protocol":
            raise failure
        if process.wait(timeout=wire._remaining(deadline)) != 0:
            raise QdrantHTTPError("worker_protocol")
        wire._remaining(deadline)
        if failure is not None:
            raise failure
        if type(value) is not dict:
            raise QdrantHTTPError("worker_protocol")
        return value
    except wire._Failure as error:
        raise QdrantHTTPError(error.code) from None
    except (subprocess.TimeoutExpired, TimeoutError):
        raise QdrantHTTPError("timeout") from None
    except OSError:
        raise QdrantHTTPError("network_error") from None
    finally:
        if process is not None:
            _finish_process(process, thread)


def request_json(config: QdrantConfig, method: str, path: str, body: dict | None,
                 *, deadline: float) -> dict:
    """Return the full Qdrant envelope within the caller's absolute monotonic budget.

    The smaller of ``deadline`` and config.timeout_seconds covers child startup,
    DNS, headers, body and JSON decoding. Cleanup has a short bounded grace.
    """
    try:
        wire._remaining(deadline)
        if not isinstance(config, QdrantConfig):
            raise QdrantHTTPError("request_invalid")
        deadline = min(deadline, time.monotonic() + config.timeout_seconds)
        try:
            key = os.environ[config.api_key_env]
        except KeyError:
            raise QdrantHTTPError("credential_missing") from None
        wire._validate_request(method, path, key)
        raw_body = None if body is None else wire._dump_object(body, wire.MAX_BODY_BYTES, deadline=deadline)
        wire._remaining(deadline)
        request = wire._dump_object({
            "url": config.url, "method": method, "path": path, "api_key": key,
            "body_b64": None if raw_body is None else base64.b64encode(raw_body).decode("ascii"),
            "timeout_seconds": wire._remaining(deadline),
        }, wire.MAX_REQUEST_FRAME, deadline=deadline)
        wire._remaining(deadline)
        return _exchange(request, deadline=deadline)
    except wire._Failure as error:
        raise QdrantHTTPError(error.code) from None
    except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
        raise QdrantHTTPError("request_invalid") from None
