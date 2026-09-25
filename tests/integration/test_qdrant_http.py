"""Real loopback and isolated-process checks for the bounded Qdrant transport."""
from __future__ import annotations

import base64
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import ssl
import subprocess
import sys
import threading
import time
import traceback

import pytest

from scope_recall.vector import qdrant_http as transport
from scope_recall.vector.qdrant_config import QdrantConfig

KEY = "test-only-qdrant-secret"
ENVELOPE = {"result": {"status": "completed"}, "status": "ok", "time": 0.01}


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setenv("SCOPE_RECALL_QDRANT_API_KEY", KEY)
    requests = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def handle_request(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append((self.command, self.path, dict(self.headers), raw))
            try:
                self.server.reply(self)
            except (OSError, ValueError):
                pass  # Deadline tests deliberately close the client socket.

        do_GET = do_PUT = do_POST = do_DELETE = handle_request

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.requests = requests
    httpd.stop = threading.Event()
    httpd.reply = lambda handler: reply(handler, json.dumps(ENVELOPE).encode())
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    httpd.config = QdrantConfig(f"http://127.0.0.1:{httpd.server_port}")
    yield httpd
    httpd.stop.set()
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=1)


def reply(handler, body=b"{}", *, status=200, headers=None):
    handler.send_response(status)
    for key, value in (headers or {}).items():
        handler.send_header(key, value)
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def call(config, method="GET", path="/collections/test", body=None, budget=2.0):
    return transport.request_json(config, method, path, body, deadline=time.monotonic() + budget)


def assert_safe(error, *, code=None, status=None):
    assert isinstance(error, transport.QdrantHTTPError)
    if code is not None:
        assert error.code == code
    assert error.status == status
    text = "".join(traceback.format_exception(error)) + repr(error) + repr(vars(error))
    for secret in (KEY, "127.0.0.1", "private-response", "qdrant-secret-host"):
        assert secret not in text
    assert error.__cause__ is None


@pytest.fixture
def children(monkeypatch):
    launched = []
    popen = subprocess.Popen

    def record(*args, **kwargs):
        child = popen(*args, **kwargs)
        launched.append((child, args, kwargs))
        return child

    monkeypatch.setattr(transport.subprocess, "Popen", record)
    yield launched
    for child, _, _ in launched:
        assert child.wait(timeout=1) is not None
        assert child.stdin.closed and child.stdout.closed


@pytest.mark.parametrize("method,body", [("GET", None), ("PUT", {"vectors": {"size": 2}}),
                                         ("POST", {"filter": {"must": []}}), ("DELETE", None)])
def test_methods_envelope_secret_stdin_and_owned_cleanup(server, children, monkeypatch, method, body):
    monkeypatch.setenv("HTTP_PROXY", "http://qdrant-secret-host:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://qdrant-secret-host:9")
    monkeypatch.setenv("ALL_PROXY", "http://qdrant-secret-host:9")
    monkeypatch.setenv("UNRELATED_SECRET", "private-response")
    assert call(server.config, method, "/collections/test?wait=true", body) == ENVELOPE
    assert len(server.requests) == len(children) == 1
    sent_method, path, headers, raw = server.requests[0]
    assert sent_method == method and path == "/collections/test?wait=true"
    assert headers["api-key"] == KEY
    assert (json.loads(raw) if raw else None) == body
    child, args, options = children[0]
    assert args[0] == [sys.executable, "-I", "-B", str(transport._WORKER_PATH)]
    assert KEY not in repr(args) + repr(options)
    assert not ({"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "UNRELATED_SECRET",
                 "SCOPE_RECALL_QDRANT_API_KEY", "PYTHONPATH"} & options["env"].keys())
    assert child.returncode == 0


@pytest.mark.parametrize("status", [301, 302, 307, 308, 401, 403, 404, 429, 500, 503])
def test_http_errors_are_sanitized_and_never_retried_or_redirected(server, children, status):
    server.reply = lambda handler: reply(handler, (KEY + "private-response").encode(), status=status,
                                        headers={"Location": server.config.url + "/stolen"})
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, "PUT", body={"points": []})
    assert_safe(caught.value, code="http_status", status=status)
    assert len(server.requests) == len(children) == 1


@pytest.mark.parametrize("body", [b"private-response", b"[]", b"null", b"1", b'{"x":NaN}',
                                  b'{"x":Infinity}', b'{"x":-Infinity}', b'{"x":1e999}', b'\xff'])
def test_invalid_non_object_and_nonfinite_json(server, body):
    server.reply = lambda handler: reply(handler, body)
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config)
    assert_safe(caught.value, code="http_protocol", status=200)


@pytest.mark.parametrize("stage", ["headers", "body"])
def test_slow_drip_has_one_absolute_deadline(server, children, stage):
    def drip(handler):
        raw = (b"HTTP/1.1 200 OK\r\nX-Slow: private-response" if stage == "headers" else
               b"HTTP/1.1 200 OK\r\nContent-Length: 10000\r\n\r\n{\"result\":\"")
        handler.connection.sendall(raw)
        while not server.stop.wait(0.025):
            handler.connection.sendall(b" ")

    server.reply = drip
    started = time.monotonic()
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, budget=0.3)
    elapsed = time.monotonic() - started
    assert_safe(caught.value, code="timeout")
    assert 0.2 <= elapsed < 0.75
    assert len(children) == 1


def fake_worker(tmp_path, monkeypatch, source):
    path = tmp_path / "worker.py"
    path.write_text(source, encoding="utf-8")
    monkeypatch.setattr(transport, "_WORKER_PATH", path)


def test_dns_stall_and_stdin_stall_are_bounded(server, children, tmp_path, monkeypatch):
    worker_path = str(transport._WORKER_PATH)
    fake_worker(tmp_path, monkeypatch,
                "import socket,time,runpy\n"
                "def stall(*a, **k):\n    time.sleep(30)\n"
                "socket.getaddrinfo=stall\n"
                f"runpy.run_path({worker_path!r}, run_name='__main__')\n")
    started = time.monotonic()
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, budget=0.25)
    assert_safe(caught.value, code="timeout")
    assert time.monotonic() - started < 0.7
    assert not server.requests
    fake_worker(tmp_path, monkeypatch, "import time\ntime.sleep(30)\n")
    started = time.monotonic()
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, "PUT", body={"data": "x" * 200_000}, budget=0.25)
    assert_safe(caught.value, code="timeout")
    assert time.monotonic() - started < 0.7
    assert len(children) == 2


def test_config_timeout_caps_later_caller_deadline(server, children):
    server.reply = lambda handler: server.stop.wait(5)
    config = QdrantConfig(server.config.url, timeout_seconds=0.2)
    started = time.monotonic()
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(config, budget=5)
    assert_safe(caught.value, code="timeout")
    assert time.monotonic() - started < 0.7


@pytest.mark.parametrize("declared", [True, False])
def test_response_limit_known_and_streamed(server, declared):
    def oversized(handler):
        handler.send_response(200)
        if declared:
            handler.send_header("Content-Length", str(16 * 1024 * 1024 + 1))
        handler.send_header("Connection", "close")
        handler.end_headers()
        if not declared:
            for _ in range(257):
                handler.wfile.write(b"x" * 65536)

    server.reply = oversized
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, budget=5)
    assert_safe(caught.value, code="response_limit", status=200)


@pytest.mark.parametrize("url", ["http://0x08080808:6333", "http://0X08080808", "http://134744072",
                                 "http://127.1", "http://010.010.010.010", "http://8.8.8.8",
                                 "https://example.test?", "https://example.test#"])
def test_helper_stdin_uses_same_origin_validation_before_connect(monkeypatch, url):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid origin reached connection")

    monkeypatch.setattr(transport.wire.http.client, "HTTPConnection", forbidden)
    monkeypatch.setattr(transport.wire.http.client, "HTTPSConnection", forbidden)
    raw = json.dumps(dict(url=url, method="GET", path="/collections", api_key=KEY,
                          body_b64=None, timeout_seconds=1)).encode()
    assert transport.wire._request(raw) == {"ok": False, "code": "request_invalid", "status": None}


@pytest.mark.parametrize("addresses", [["8.8.8.8"], ["::ffff:8.8.8.8"], ["127.0.0.1", "8.8.8.8"]])
def test_http_name_resolution_is_checked_before_connect(monkeypatch, addresses):
    infos = [(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM, 6, "",
              (host, 6333, 0, 0) if ":" in host else (host, 6333)) for host in addresses]
    monkeypatch.setattr(transport.wire.socket, "getaddrinfo", lambda *a, **kw: infos)

    def forbidden(*args, **kwargs):
        pytest.fail("public HTTP destination reached socket creation")

    monkeypatch.setattr(transport.wire.socket, "socket", forbidden)
    raw = json.dumps(dict(url="http://vectors:6333", method="GET", path="/collections", api_key=KEY,
                          body_b64=None, timeout_seconds=1)).encode()
    assert transport.wire._request(raw) == {"ok": False, "code": "request_invalid", "status": None}


def test_internal_dns_connects_checked_address_once(server, monkeypatch):
    real_getaddrinfo = socket.getaddrinfo
    calls = []

    def resolve(host, port, *args, **kwargs):
        calls.append(host)
        return real_getaddrinfo("127.0.0.1", server.server_port, *args, **kwargs)

    monkeypatch.setattr(transport.wire.socket, "getaddrinfo", resolve)
    raw = json.dumps(dict(url=f"http://vectors:{server.server_port}", method="GET", path="/collections",
                          api_key=KEY, body_b64=None, timeout_seconds=1)).encode()
    result = transport.wire._request(raw)
    assert result["ok"] is True
    assert calls == ["vectors"]
    assert server.requests[0][2]["api-key"] == KEY


def test_request_limit_and_finite_object_before_spawn(server, children):
    for body, code in [({"x": "x" * (8 * 1024 * 1024)}, "request_limit"),
                       ({"x": float("nan")}, "request_invalid"),
                       ({"x": float("inf")}, "request_invalid"), ([], "request_invalid")]:
        with pytest.raises(transport.QdrantHTTPError) as caught:
            call(server.config, "PUT", body=body)
        assert_safe(caught.value, code=code)
    assert not children and not server.requests


@pytest.mark.parametrize("deadline", [0, float("nan"), float("inf"), True, "private-response"])
def test_deadline_rejected_before_spawn(server, children, deadline):
    with pytest.raises(transport.QdrantHTTPError) as caught:
        transport.request_json(server.config, "GET", "/collections/test", None, deadline=deadline)
    assert_safe(caught.value, code="timeout")
    assert not children


@pytest.mark.parametrize("method,path", [("PATCH", "/"), ("GET\r\n", "/"), ("GET", "//host/"),
                                        ("GET", "https://qdrant-secret-host/"), ("GET", "/\r\n"),
                                        ("GET", "/#secret"), ("GET", "/\\secret")])
def test_method_path_validation(server, children, method, path):
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, method, path)
    assert_safe(caught.value, code="request_invalid")
    assert not children


@pytest.mark.parametrize("key", [None, "", "abc\r\nInjected: value", "x" * 8193, "秘密"])
def test_invalid_or_missing_credential(server, children, monkeypatch, key):
    if key is None:
        monkeypatch.delenv(server.config.api_key_env)
    else:
        monkeypatch.setenv(server.config.api_key_env, key)
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config)
    assert_safe(caught.value, code="credential_missing" if key is None else "credential_invalid")
    assert not children


def test_connection_refused_sanitized_and_only_own_process_reaped(server, children):
    # Binding without listening reserves a loopback port which rejects connections.
    with contextlib.closing(socket.socket()) as reserved:
        reserved.bind(("127.0.0.1", 0))
        config = QdrantConfig(f"http://127.0.0.1:{reserved.getsockname()[1]}")
        popen = subprocess.Popen
        sibling = popen([sys.executable, "-I", "-B", "-c", "import time; time.sleep(30)"],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            with pytest.raises(transport.QdrantHTTPError) as caught:
                call(config)
            assert_safe(caught.value, code="network_error")
            assert sibling.poll() is None
        finally:
            sibling.kill()
            sibling.communicate(timeout=1)


@pytest.mark.parametrize("source", [
    "import sys; sys.stdin.buffer.read(); print('private-response')",
    "import sys; sys.stdin.buffer.read(); print('{\"ok\":false,\"code\":\"private-response\"}')",
    "import sys; sys.stdin.buffer.read(); print('{\"ok\":true,\"status\":200,\"body\":[]}')",
    "import sys; sys.stdin.buffer.read(); print('{\"ok\":true,\"status\":200,\"body\":{\"x\":1e999}}')",
    "import sys; sys.stdin.buffer.read(); sys.exit(3)",
])
def test_bad_worker_frame_is_sanitized(server, children, tmp_path, monkeypatch, source):
    fake_worker(tmp_path, monkeypatch, source + "\n")
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config)
    assert_safe(caught.value, code="worker_protocol")


def test_worker_output_is_bounded_before_json_parse(server, children, tmp_path, monkeypatch):
    fake_worker(tmp_path, monkeypatch,
                "import sys,time\nsys.stdin.buffer.read()\n"
                "while True:\n    sys.stdout.buffer.write(b'x' * 65536)\n    sys.stdout.buffer.flush()\n")
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, budget=5)
    assert_safe(caught.value, code="worker_protocol")


def test_truncated_body_cannot_masquerade_as_valid_json(server):
    def truncated(handler):
        handler.connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{}")
        handler.close_connection = True

    server.reply = truncated
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config)
    assert_safe(caught.value, code="http_protocol", status=200)


def test_chunked_object_and_exact_size_limits(server):
    def chunked(handler):
        body = json.dumps(ENVELOPE).encode()
        handler.connection.sendall(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                                   + f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n")
    server.reply = chunked
    assert call(server.config) == ENVELOPE
    exact_response = b'{"x":"' + b"x" * (16 * 1024 * 1024 - 8) + b'"}'
    assert len(exact_response) == 16 * 1024 * 1024
    server.reply = lambda handler: reply(handler, exact_response)
    assert len(call(server.config, budget=5)["x"]) == 16 * 1024 * 1024 - 8
    server.reply = lambda handler: reply(handler)
    body = {"x": "x" * (8 * 1024 * 1024 - 8)}
    assert call(server.config, "PUT", body=body, budget=5) == {}
    assert len(server.requests[-1][3]) == 8 * 1024 * 1024


@pytest.mark.parametrize("headers", [b"Content-Length: -1", b"Content-Length: invalid",
                                     b"Content-Length: 2\r\nContent-Length: 2",
                                     b"Content-Length: 2\r\nTransfer-Encoding: chunked",
                                     b"Transfer-Encoding: gzip", b"Content-Encoding: gzip"])
def test_ambiguous_http_framing_is_rejected(server, headers):
    def malformed(handler):
        handler.connection.sendall(b"HTTP/1.1 200 OK\r\n" + headers
                                   + b"\r\nConnection: close\r\n\r\n{}")
        handler.close_connection = True
    server.reply = malformed
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config)
    assert_safe(caught.value, code="http_protocol", status=200)


def test_isolated_helper_rejects_stdin_overflow_and_emits_only_json(server):
    child = subprocess.run([sys.executable, "-I", "-B", str(transport._WORKER_PATH)],
                           input=b"x" * (transport.wire.MAX_REQUEST_FRAME + 1),
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={}, timeout=3)
    assert child.returncode == 0 and child.stderr == b""
    assert json.loads(child.stdout) == {"ok": False, "code": "request_limit", "status": None}


def test_tls_validates_certificate_and_ignores_ambient_ca_override(server, tmp_path, monkeypatch):
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-keyout", str(key), "-out", str(cert), "-subj", "/CN=localhost",
                    "-addext", "subjectAltName=IP:127.0.0.1"], check=True, capture_output=True, timeout=5)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    monkeypatch.setenv("SSL_CERT_FILE", str(cert))
    config = QdrantConfig(server.config.url.replace("http:", "https:"))
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(config)
    assert_safe(caught.value, code="network_error")
    assert not server.requests


def test_worker_exit_is_inside_deadline(server, children, tmp_path, monkeypatch):
    fake_worker(tmp_path, monkeypatch,
                "import os,sys,time\nsys.stdin.buffer.read()\n"
                "sys.stdout.buffer.write(b'{\"ok\":true,\"status\":200,\"body_b64\":\"e30=\"}')\n"
                "sys.stdout.buffer.flush()\nos.close(1)\ntime.sleep(30)\n")
    started = time.monotonic()
    with pytest.raises(transport.QdrantHTTPError) as caught:
        call(server.config, budget=0.25)
    assert_safe(caught.value, code="timeout")
    assert time.monotonic() - started < 0.7


def test_helper_frame_contract_is_duration_based(server):
    def run(frame):
        child = subprocess.run([sys.executable, "-I", "-B", str(transport._WORKER_PATH)],
                               input=json.dumps(frame).encode(), stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env={}, timeout=3)
        assert child.returncode == 0 and child.stderr == b""
        return json.loads(child.stdout)

    base = {"url": server.config.url, "method": "GET", "path": "/collections/test",
            "api_key": KEY, "body_b64": None, "timeout_seconds": 2.0}
    ok = run(base)
    assert ok["ok"] is True and json.loads(base64.b64decode(ok["body_b64"])) == ENVELOPE
    assert server.requests[0][2]["api-key"] == KEY
    broken = [{**base, "deadline": 1.0},
              {name: value for name, value in base.items() if name != "timeout_seconds"},
              {**base, "timeout_seconds": 0},
              {**base, "api_key": None}, {**base, "method": "PATCH"}, {**base, "path": "relative"}]
    for frame in broken:
        result = run(frame)
        assert result["ok"] is False and result["status"] is None
        assert result["code"] in {"request_invalid", "credential_invalid", "timeout"}
        assert KEY not in json.dumps(result)
    # A non-finite number is not JSON this side accepts, so the frame itself is the fault.
    nonfinite = run({**base, "timeout_seconds": float("inf")})
    assert nonfinite == {"ok": False, "code": "http_protocol", "status": None}
    assert not any(KEY in value for value in nonfinite.values() if isinstance(value, str))
