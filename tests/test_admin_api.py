import json
import threading
import time
import urllib.error
import urllib.request

import pytest

from kurd import Router, set_admin_token, clear_admin_token
from kurd._kurd import start_http_gateway, stop_http_gateway

_PORT = 18732
_BASE = f"http://127.0.0.1:{_PORT}"


def _get(path: str, token: str | None = None) -> dict:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{_BASE}{path}", headers=headers)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def _post(path: str, body: dict, token: str | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{_BASE}{path}", data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        return resp.status, json.loads(resp.read())


def _delete(path: str, token: str | None = None) -> tuple[int, dict]:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{_BASE}{path}", headers=headers, method="DELETE")
    with urllib.request.urlopen(req) as resp:
        return resp.status, json.loads(resp.read())


@pytest.fixture(scope="module", autouse=True)
def gateway():
    router = Router()

    @router.tool(name="multiply")
    async def multiply(a: int, b: int) -> int:
        return a * b

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
# GET /admin/servers
# ---------------------------------------------------------------------------

def test_admin_list_servers_empty():
    data = _get("/admin/servers")
    assert "servers" in data
    assert isinstance(data["servers"], list)
    assert data["count"] == 0


def test_admin_add_server_created():
    status, data = _post("/admin/servers", {"name": "echo", "url": "http://localhost:19999/mcp"})
    assert status == 201
    assert data["name"] == "echo"
    assert data["replaced"] is False


def test_admin_list_servers_shows_added():
    data = _get("/admin/servers")
    names = [s["name"] for s in data["servers"]]
    assert "echo" in names


def test_admin_add_server_replace_existing():
    status, data = _post("/admin/servers", {"name": "echo", "url": "http://localhost:19998/mcp"})
    assert status == 200
    assert data["replaced"] is True


def test_admin_delete_server():
    status, data = _delete("/admin/servers/echo")
    assert status == 200
    assert data["deleted"] == "echo"


def test_admin_delete_server_not_found():
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        _delete("/admin/servers/nonexistent")
    assert exc_info.value.code == 404


def test_admin_add_server_invalid_url():
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        _post("/admin/servers", {"name": "bad", "url": "ftp://not-valid"})
    assert exc_info.value.code == 400


def test_admin_add_server_missing_name():
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        _post("/admin/servers", {"name": "", "url": "http://localhost:19999/mcp"})
    assert exc_info.value.code == 400


# ---------------------------------------------------------------------------
# GET /admin/tools
# ---------------------------------------------------------------------------

def test_admin_list_tools_includes_local():
    data = _get("/admin/tools")
    assert "tools" in data
    names = [t["name"] for t in data["tools"]]
    assert "multiply" in names
    sources = [t["source"] for t in data["tools"] if t["name"] == "multiply"]
    assert sources == ["local"]


def test_admin_list_tools_counts():
    data = _get("/admin/tools")
    assert data["localCount"] >= 1
    assert data["count"] == data["localCount"] + data["upstreamCount"]


# ---------------------------------------------------------------------------
# POST /admin/tools/reload
# ---------------------------------------------------------------------------

def test_admin_reload_tools():
    status, data = _post("/admin/tools/reload", {})
    assert status == 200
    assert data["reloaded"] is True


# ---------------------------------------------------------------------------
# Admin token auth
# ---------------------------------------------------------------------------

def test_admin_token_rejects_wrong_token():
    set_admin_token("admin-secret")
    try:
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _get("/admin/servers", token="wrong-token")
        assert exc_info.value.code == 401
    finally:
        clear_admin_token()


def test_admin_token_accepts_correct_token():
    set_admin_token("admin-secret")
    try:
        data = _get("/admin/servers", token="admin-secret")
        assert "servers" in data
    finally:
        clear_admin_token()


def test_admin_token_cleared_open_again():
    set_admin_token("admin-secret")
    clear_admin_token()
    # After clearing, no token needed
    data = _get("/admin/servers")
    assert "servers" in data
