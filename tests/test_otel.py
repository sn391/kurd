"""
P4 tests: real OpenTelemetry export / W3C traceparent propagation.

Covers:
- Every MCP response carries a ``traceparent`` header
- ``traceparent`` format is valid W3C (00-<32hex>-<16hex>-01)
- Incoming ``traceparent`` is respected (trace_id preserved, new span_id issued)
- configure_otel / clear_otel toggle export without affecting correctness
- Router.configure_otel / Router.clear_otel are wired through
"""
import json
import re
import threading
import time
import urllib.request
import urllib.error

import pytest

from kurd import Router, configure_otel, clear_otel
from kurd._kurd import start_http_gateway, stop_http_gateway

_PORT = 18735
_BASE = f"http://127.0.0.1:{_PORT}"

_TRACEPARENT_RE = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")


def _mcp(payload: dict, headers: dict | None = None) -> tuple[dict, dict]:
    """Returns (response_body, response_headers)."""
    data = json.dumps(payload).encode()
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(f"{_BASE}/mcp", data=data, headers=req_headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read()), dict(resp.headers)


def _list_tools(extra_headers: dict | None = None) -> tuple[dict, dict]:
    return _mcp({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers=extra_headers)


@pytest.fixture(scope="module", autouse=True)
def gateway():
    router = Router()

    @router.tool(name="ping")
    async def ping() -> str:
        """Ping pong"""
        return "pong"

    thread = threading.Thread(
        target=start_http_gateway,
        args=(f"127.0.0.1:{_PORT}",),
        daemon=True,
    )
    thread.start()

    for _ in range(20):
        try:
            urllib.request.urlopen(f"{_BASE}/health", timeout=1)
            break
        except Exception:
            time.sleep(0.1)

    yield

    stop_http_gateway()


# ---------------------------------------------------------------------------
# traceparent header presence and format
# ---------------------------------------------------------------------------

def test_response_includes_traceparent():
    _, headers = _list_tools()
    assert "traceparent" in {k.lower() for k in headers}, \
        f"traceparent missing from response headers: {list(headers.keys())}"


def test_traceparent_format_is_valid():
    _, headers = _list_tools()
    tp = next(v for k, v in headers.items() if k.lower() == "traceparent")
    assert _TRACEPARENT_RE.match(tp), f"invalid traceparent: {tp!r}"


def test_traceparent_version_is_00():
    _, headers = _list_tools()
    tp = next(v for k, v in headers.items() if k.lower() == "traceparent")
    assert tp.startswith("00-"), f"expected version 00, got: {tp}"


def test_traceparent_flags_are_01():
    _, headers = _list_tools()
    tp = next(v for k, v in headers.items() if k.lower() == "traceparent")
    assert tp.endswith("-01"), f"expected sampled flag 01, got: {tp}"


def test_consecutive_requests_have_different_trace_ids():
    _, h1 = _list_tools()
    _, h2 = _list_tools()
    tp1 = next(v for k, v in h1.items() if k.lower() == "traceparent")
    tp2 = next(v for k, v in h2.items() if k.lower() == "traceparent")
    tid1 = tp1.split("-")[1]
    tid2 = tp2.split("-")[1]
    assert tid1 != tid2, "consecutive requests must have different trace IDs"


# ---------------------------------------------------------------------------
# Incoming traceparent propagation
# ---------------------------------------------------------------------------

def test_incoming_trace_id_is_preserved():
    """If the caller sends a traceparent the gateway must keep the same trace_id."""
    upstream_trace_id = "a" * 32
    upstream_span_id = "b" * 16
    incoming_tp = f"00-{upstream_trace_id}-{upstream_span_id}-01"

    _, headers = _list_tools(extra_headers={"traceparent": incoming_tp})
    tp = next(v for k, v in headers.items() if k.lower() == "traceparent")
    returned_trace_id = tp.split("-")[1]
    assert returned_trace_id == upstream_trace_id, \
        f"expected trace_id {upstream_trace_id}, got {returned_trace_id}"


def test_incoming_span_id_is_replaced():
    """The gateway creates a NEW span_id even when it inherits the trace_id."""
    upstream_trace_id = "c" * 32
    upstream_span_id = "d" * 16
    incoming_tp = f"00-{upstream_trace_id}-{upstream_span_id}-01"

    _, headers = _list_tools(extra_headers={"traceparent": incoming_tp})
    tp = next(v for k, v in headers.items() if k.lower() == "traceparent")
    returned_span_id = tp.split("-")[2]
    assert returned_span_id != upstream_span_id, \
        "gateway must issue a new span_id, not echo the parent's span_id"


def test_malformed_traceparent_is_ignored():
    """A malformed traceparent must not crash the gateway; a fresh trace starts."""
    _, headers = _list_tools(extra_headers={"traceparent": "not-valid"})
    tp = next((v for k, v in headers.items() if k.lower() == "traceparent"), None)
    assert tp is not None, "gateway must still return a traceparent after bad input"
    assert _TRACEPARENT_RE.match(tp), f"fresh traceparent should be valid: {tp!r}"


# ---------------------------------------------------------------------------
# configure_otel / clear_otel (module-level API)
# ---------------------------------------------------------------------------

def test_configure_otel_does_not_break_requests():
    """configure_otel with an unreachable endpoint must not affect response correctness."""
    configure_otel("http://127.0.0.1:19999", "test-svc")
    try:
        body, headers = _list_tools()
        assert "result" in body
        tp = next((v for k, v in headers.items() if k.lower() == "traceparent"), None)
        assert tp is not None
    finally:
        clear_otel()


def test_clear_otel_does_not_break_requests():
    configure_otel("http://127.0.0.1:19999", "test-svc")
    clear_otel()
    body, headers = _list_tools()
    assert "result" in body
    # traceparent must still be present (tracing context is always emitted)
    assert any(k.lower() == "traceparent" for k in headers)


# ---------------------------------------------------------------------------
# Router.configure_otel / Router.clear_otel
# ---------------------------------------------------------------------------

def test_router_configure_otel():
    """Router.configure_otel wires through without error."""
    router = Router()
    router.configure_otel("http://127.0.0.1:19999", "router-svc")
    body, _ = _list_tools()
    assert "result" in body
    router.clear_otel()
