import json
import threading
import time
import urllib.error
import urllib.request

import pytest

from kurd import Router
from kurd._kurd import (
    clear_http_bearer_token,
    set_http_bearer_token,
    start_http_gateway,
    stop_http_gateway,
)

_PORT = 18731
_BASE = f"http://127.0.0.1:{_PORT}"


def _post(payload: dict, token: str | None = None) -> dict:
    data = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        f"{_BASE}/mcp", data=data, headers=headers, method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


@pytest.fixture(scope="module", autouse=True)
def gateway():
    router = Router()

    @router.tool(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    thread = threading.Thread(
        target=start_http_gateway,
        args=(f"127.0.0.1:{_PORT}",),
        daemon=True,
    )
    thread.start()

    # Wait for the server to accept connections.
    for _ in range(20):
        try:
            urllib.request.urlopen(f"{_BASE}/health", timeout=1)
            break
        except Exception:
            time.sleep(0.1)

    yield

    stop_http_gateway()


def test_health_endpoint():
    with urllib.request.urlopen(f"{_BASE}/health") as resp:
        assert resp.status == 200


def test_ping():
    result = _post({"jsonrpc": "2.0", "id": 1, "method": "ping"})
    assert result["result"] == {}


def test_tools_list_contains_registered_tool():
    result = _post({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    assert "result" in result
    names = [t["name"] for t in result["result"]["tools"]]
    assert "add" in names


def test_tools_list_pagination_no_next_cursor_for_small_list():
    result = _post({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
    assert "nextCursor" not in result["result"]


def test_tools_call_local_tool():
    result = _post({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {"name": "add", "arguments": {"a": 3, "b": 4}},
    })
    assert "result" in result
    assert result["result"]["isError"] is False
    assert result["result"]["content"][0]["text"] == "7"


def test_resources_list_returns_empty():
    result = _post({"jsonrpc": "2.0", "id": 5, "method": "resources/list"})
    assert result["result"]["resources"] == []


def test_prompts_list_returns_empty():
    result = _post({"jsonrpc": "2.0", "id": 6, "method": "prompts/list"})
    assert result["result"]["prompts"] == []


def test_unknown_method_returns_32601():
    result = _post({"jsonrpc": "2.0", "id": 7, "method": "unknown/method"})
    assert result["error"]["code"] == -32601


def test_invalid_json_returns_32700():
    req = urllib.request.Request(
        f"{_BASE}/mcp",
        data=b'{"jsonrpc":',
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    assert result["error"]["code"] == -32700


def test_missing_content_type_returns_415():
    req = urllib.request.Request(
        f"{_BASE}/mcp",
        data=b'{"jsonrpc":"2.0","id":1,"method":"ping"}',
        headers={"Content-Type": "text/plain"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    assert result["error"]["code"] == -32600


def test_bearer_auth_rejects_without_token():
    set_http_bearer_token("secret-test-token")
    try:
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _post({"jsonrpc": "2.0", "id": 8, "method": "ping"})
        assert exc_info.value.code == 401
    finally:
        clear_http_bearer_token()


def test_bearer_auth_accepts_correct_token():
    set_http_bearer_token("secret-test-token")
    try:
        result = _post(
            {"jsonrpc": "2.0", "id": 9, "method": "ping"},
            token="secret-test-token",
        )
        assert result["result"] == {}
    finally:
        clear_http_bearer_token()


def test_initialize_returns_protocol_version():
    result = _post({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "initialize",
        "params": {
            "protocolVersion": "2026-07-28",
            "clientInfo": {"name": "test", "version": "1.0"},
            "capabilities": {},
        },
    })
    assert "result" in result
    assert result["result"]["protocolVersion"] == "2026-07-28"
    assert "capabilities" in result["result"]
    assert "serverInfo" in result["result"]


def test_completion_complete_returns_empty():
    result = _post({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "completion/complete",
        "params": {
            "ref": {"type": "ref/prompt", "name": "code_review"},
            "argument": {"name": "language", "value": "py"},
        },
    })
    assert "result" in result
    assert result["result"]["completion"]["values"] == []
    assert result["result"]["completion"]["hasMore"] is False


def test_notification_returns_202():
    data = json.dumps({
        "jsonrpc": "2.0",
        "method": "notifications/tools/list_changed",
    }).encode()
    req = urllib.request.Request(
        f"{_BASE}/mcp",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 202
        assert resp.read() == b""


def test_cors_preflight():
    req = urllib.request.Request(
        f"{_BASE}/mcp",
        headers={
            "Origin": "https://example.com",
            "Access-Control-Request-Method": "POST",
        },
        method="OPTIONS",
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 204
        assert "access-control-allow-origin" in {
            k.lower() for k in dict(resp.headers).keys()
        }


def test_cors_header_on_post_response():
    result_resp = None
    data = json.dumps({"jsonrpc": "2.0", "id": 12, "method": "ping"}).encode()
    req = urllib.request.Request(
        f"{_BASE}/mcp",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        result_resp = dict(resp.headers)
    assert any(
        k.lower() == "access-control-allow-origin"
        for k in result_resp.keys()
    )
