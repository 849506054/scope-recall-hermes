"""Private, one-shot stdlib HTTP worker. Its parent owns the total deadline."""
from __future__ import annotations

import base64
import http.client
import ipaddress
import json
import math
import re
import socket
import ssl
import sys
import time
from urllib.parse import urlsplit

MAX_BODY_BYTES = 8 * 1024 * 1024
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_REQUEST_FRAME = (MAX_BODY_BYTES * 4) // 3 + 65536
MAX_RESPONSE_FRAME = (MAX_RESPONSE_BYTES * 4) // 3 + 1024
MAX_KEY_BYTES = 8192
ERROR_CODES = frozenset({
    "request_invalid", "request_limit", "credential_missing", "credential_invalid",
    "http_status", "http_protocol", "network_error", "timeout", "response_limit", "worker_protocol",
})


class _Failure(Exception):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _reject_constant(_value):
    raise ValueError("json_number")


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("json_number")
    return result


def _load_object(raw):
    result = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant, parse_float=_finite_float)
    if type(result) is not dict:
        raise ValueError("json_object")
    return result


def _bounded_shape(value, limit, *, deadline):
    """Refuse a body whose shape cannot be encoded inside the budget or the limit.

    ``json`` emits a whole string literal at once, so one huge string can consume
    the budget before any per-piece check runs: a string longer than the limit can
    never fit, and a body with more nodes than the limit has bytes cannot either.
    Lengths are read rather than encoded, so every step stays interruptible, and a
    legitimate body is left to the encoder's exact per-piece limit.
    """
    seen = 0
    stack = [value]
    while stack:
        item = stack.pop()
        seen += 1
        if seen % 512 == 0:
            _remaining(deadline)
        kind = type(item)
        if kind is str:
            if len(item) > limit:
                raise _Failure("request_limit")
        elif kind is dict:
            stack.extend(item.keys())
            stack.extend(item.values())
        elif kind is list:
            stack.extend(item)
        elif kind is not bool and item is not None and kind is not int and kind is not float:
            raise _Failure("request_invalid")
        # Every visited node costs at least one encoded byte, so a body with more
        # nodes than bytes cannot fit. A fixed node count refused legitimate
        # batches: 64 points at 2048 dimensions is 263,619 nodes in 1.8 MB.
        if seen > limit:
            raise _Failure("request_limit")


def _dump_object(value, limit, *, deadline):
    if type(value) is not dict:
        raise _Failure("request_invalid")
    _bounded_shape(value, limit, deadline=deadline)
    result = bytearray()
    for piece in json.JSONEncoder(ensure_ascii=True, allow_nan=False, separators=(",", ":")).iterencode(value):
        _remaining(deadline)
        encoded = piece.encode("ascii")
        if len(result) + len(encoded) > limit:
            raise _Failure("request_limit")
        result.extend(encoded)
    return bytes(result)


def _remaining(deadline):
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise _Failure("timeout")
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _Failure("timeout")
    return remaining


def _validate_request(method, path, key):
    if (type(method) is not str or method not in {"GET", "PUT", "POST", "DELETE"}
            or type(path) is not str or not path.startswith("/") or path.startswith("//")
            or len(path) > 8192 or not path.isascii()
            or any(ord(char) <= 32 or ord(char) == 127 for char in path)
            or "#" in path or "\\" in path):
        raise _Failure("request_invalid")
    if (type(key) is not str or not key or len(key) > MAX_KEY_BYTES
            or not key.isascii() or any(ord(char) <= 32 or ord(char) == 127 for char in key)):
        raise _Failure("credential_invalid")


_INTERNAL_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"), ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"), ipaddress.ip_network("fc00::/7"),
)
_DNS_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
# Alternative numeric notations (hex, bare decimal, leading zeros) that a C resolver
# reads as an address although they are not a dotted quad.
_NUMERIC_LABEL = re.compile(r"(?:0[xX][0-9A-Fa-f]+|[0-9]+)")


def _internal_literal(host):
    """True/False for an address literal; None when the host is not one."""
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return None
    return address.is_loopback or any(address in network for network in _INTERNAL_NETWORKS)


def valid_origin(value):
    """The one origin decision, shared by this worker and the config layer.

    This module is the process that dials, so it owns the rule; ``qdrant_config``
    imports it rather than keeping a second copy. A bare http(s) origin only;
    plaintext is allowed only for an explicitly internal destination, and a host
    written in an alternative numeric notation is refused instead of trusted.
    """
    if (type(value) is not str or len(value) > 2048 or not value.isascii()
            or any(ord(char) <= 32 or ord(char) == 127 for char in value)
            or any(char in value for char in "?#\\%")):
        raise ValueError("origin")
    try:
        parsed = urlsplit(value)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ValueError("origin") from None
    if (parsed.scheme not in {"http", "https"} or not host or parsed.username is not None
            or parsed.password is not None or parsed.path not in {"", "/"}
            or parsed.netloc.endswith(":") or port == 0):
        raise ValueError("origin")
    internal = _internal_literal(host)
    if internal is None:
        labels = host.split(".")
        if (len(host) > 253 or any(not _DNS_LABEL.fullmatch(label) for label in labels)
                or all(_NUMERIC_LABEL.fullmatch(label) for label in labels)):
            raise ValueError("origin")
        internal = "." not in host
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    if parsed.scheme == "http" and not internal:
        raise ValueError("origin")
    return parsed, host, port


def _internal_destination(host, port):
    """Resolve a plaintext destination once and return only a checked internal address."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError:
        raise _Failure("network_error") from None
    if not infos:
        raise _Failure("network_error")
    for info in infos:
        if _internal_literal(str(info[4][0])) is not True:
            raise _Failure("request_invalid")
    return infos[0]


def _parse_request(raw):
    request = _load_object(raw)
    if set(request) != {"url", "method", "path", "api_key", "body_b64", "timeout_seconds"}:
        raise _Failure("request_invalid")
    _validate_request(request["method"], request["path"], request["api_key"])
    timeout_seconds = request["timeout_seconds"]
    if (type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0):
        raise _Failure("timeout")
    # A duration crosses the process boundary portably; this clock starts at once.
    deadline = time.monotonic() + float(timeout_seconds)
    url = request["url"]
    try:
        parsed, host, port = valid_origin(url)
    except ValueError:
        raise _Failure("request_invalid") from None
    body = None
    if request["body_b64"] is not None:
        if type(request["body_b64"]) is not str:
            raise _Failure("request_invalid")
        body = base64.b64decode(request["body_b64"], validate=True)
        if len(body) > MAX_BODY_BYTES:
            raise _Failure("request_limit")
        _load_object(body)
    return request, parsed, host, port, body, deadline


def _arm(connection, deadline):
    remaining = _remaining(deadline)
    connection.timeout = remaining
    if connection.sock is not None:
        connection.sock.settimeout(remaining)


def _response_body(response, connection, deadline):
    lengths = response.headers.get_all("Content-Length", [])
    encodings = response.headers.get_all("Transfer-Encoding", [])
    if (len(lengths) > 1 or (lengths and encodings)
            or (encodings and encodings != ["chunked"])
            or response.headers.get("Content-Encoding", "identity").lower() != "identity"):
        raise _Failure("http_protocol")
    expected = None
    if lengths:
        if not lengths[0].isascii() or not lengths[0].isdigit():
            raise _Failure("http_protocol")
        expected = int(lengths[0])
        if expected > MAX_RESPONSE_BYTES:
            raise _Failure("response_limit")
    result = bytearray()
    while True:
        _arm(connection, deadline)
        piece = response.read(min(65536, MAX_RESPONSE_BYTES - len(result) + 1))
        if not piece:
            break
        if len(result) + len(piece) > MAX_RESPONSE_BYTES:
            raise _Failure("response_limit")
        result.extend(piece)
    if expected is not None and len(result) != expected:
        raise _Failure("http_protocol")
    _load_object(bytes(result))
    _remaining(deadline)
    return bytes(result)


def _request(raw):
    connection = None
    status = None
    try:
        request, parsed, host, port, body, deadline = _parse_request(raw)
        if parsed.scheme == "https":
            connection = http.client.HTTPSConnection(host, port, context=ssl.create_default_context())
        else:
            # Dial the address that was checked: a second lookup could repoint the name.
            family, socktype, protocol, _canonical, sockaddr = _internal_destination(host, port)
            connection = http.client.HTTPConnection(host, port)
            connection.sock = socket.socket(family, socktype, protocol)
            connection.sock.settimeout(_remaining(deadline))
            connection.sock.connect(sockaddr)
        # http.client uses a direct connection and never follows redirects or environment proxies.
        _arm(connection, deadline)
        if connection.sock is None:
            connection.connect()
        _arm(connection, deadline)
        connection.request(request["method"], request["path"], body=body, headers={
            "api-key": request["api_key"], "Content-Type": "application/json",
            "Accept": "application/json", "Accept-Encoding": "identity", "Connection": "close",
        })
        _arm(connection, deadline)
        with connection.getresponse() as response:
            status = response.status
            if not 200 <= status < 300:
                raise _Failure("http_status")
            raw_body = _response_body(response, connection, deadline)
        return {"ok": True, "status": status, "body_b64": base64.b64encode(raw_body).decode("ascii")}
    except _Failure as error:
        code = error.code
    except (socket.timeout, TimeoutError):
        code = "timeout"
    except OSError:
        code = "network_error"
    except (ValueError, TypeError, UnicodeError, RecursionError, http.client.HTTPException):
        code = "http_protocol"
    except Exception:
        code = "worker_protocol"
    finally:
        if connection is not None:
            connection.close()
    return {"ok": False, "code": code, "status": status}


def main():
    try:
        raw = sys.stdin.buffer.read(MAX_REQUEST_FRAME + 1)
        result = (_request(raw) if len(raw) <= MAX_REQUEST_FRAME else
                  {"ok": False, "code": "request_limit", "status": None})
        frame = json.dumps(result, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
        if len(frame) > MAX_RESPONSE_FRAME:
            frame = b'{"ok":false,"code":"response_limit","status":null}'
        sys.stdout.buffer.write(frame)
        sys.stdout.buffer.flush()
    except Exception:
        # Tracebacks may contain endpoint, request or credential values. Stderr stays empty.
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
