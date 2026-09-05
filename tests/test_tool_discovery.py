"""
P3 tests: client-requested tool filtering and discovery metadata.

Covers:
- params.filter.namespace  — client scopes tools/list to one upstream
- params.filter.search     — keyword search in name/description
- combined filters
- _kurd metadata (available vs returned counts)
- filter runs after tenant restrictions (cannot bypass P2 policy)
- GET /admin/tools/namespaces
"""
import json
import threading
import time
import urllib.request
import urllib.error

import pytest

from kurd import Router, TenantManager
from kurd._kurd import start_http_gateway, stop_http_gateway

_PORT = 18734
_BASE = f"http://127.0.0.1:{_PORT}"

_router: Router = None


def _mcp(payload: dict, token: str | None = None) -> dict:
    data = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{_BASE}/mcp", data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def _list_tools(filter_params: dict | None = None, token: str | None = None) -> dict:
    params = {}
    if filter_params:
        params["filter"] = filter_params
    result = _mcp(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list",
         "params": params if params else {}},
        token=token,
    )
    assert "result" in result, result
    return result["result"]


def _get(path: str) -> dict:
    with urllib.request.urlopen(f"{_BASE}{path}") as resp:
        return json.loads(resp.read())


@pytest.fixture(scope="module", autouse=True)
def gateway():
    global _router
    _router = Router()

    @_router.tool(name="add")
    async def add(a: int, b: int) -> int:
        """Add two numbers together"""
        return a + b

    @_router.tool(name="multiply")
    async def multiply(a: int, b: int) -> int:
        """Multiply two numbers"""
        return a * b

    @_router.tool(name="file_read")
    async def file_read(path: str) -> str:
        """Read a file from disk"""
        return ""

    @_router.tool(name="file_write")
    async def file_write(path: str, content: str) -> bool:
        """Write content to a file on disk"""
        return True

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
# Baseline
# ---------------------------------------------------------------------------

def test_no_filter_returns_all_tools():
    result = _list_tools()
    names = [t["name"] for t in result["tools"]]
    assert "add" in names
    assert "multiply" in names
    assert "file_read" in names
    assert "file_write" in names


def test_kurd_metadata_present():
    result = _list_tools()
    assert "_kurd" in result
    assert "available" in result["_kurd"]
    assert "returned" in result["_kurd"]
    assert result["_kurd"]["available"] == result["_kurd"]["returned"]


# ---------------------------------------------------------------------------
# Namespace filter
# ---------------------------------------------------------------------------

def test_namespace_filter_unknown_returns_empty():
    result = _list_tools({"namespace": "nonexistent"})
    assert result["tools"] == []
    assert result["_kurd"]["returned"] == 0


# ---------------------------------------------------------------------------
# Search filter
# ---------------------------------------------------------------------------

def test_search_filter_by_name_substring():
    result = _list_tools({"search": "file"})
    names = [t["name"] for t in result["tools"]]
    assert "file_read" in names
    assert "file_write" in names
    assert "add" not in names
    assert "multiply" not in names


def test_search_filter_case_insensitive():
    result = _list_tools({"search": "FILE"})
    names = [t["name"] for t in result["tools"]]
    assert "file_read" in names
    assert "file_write" in names


def test_search_filter_by_description():
    result = _list_tools({"search": "disk"})
    names = [t["name"] for t in result["tools"]]
    assert "file_read" in names
    assert "file_write" in names
    assert "add" not in names


def test_search_filter_no_match_returns_empty():
    result = _list_tools({"search": "zzznonexistent"})
    assert result["tools"] == []
    assert result["_kurd"]["returned"] == 0


def test_search_filter_single_word():
    result = _list_tools({"search": "multiply"})
    names = [t["name"] for t in result["tools"]]
    assert "multiply" in names
    assert "add" not in names


# ---------------------------------------------------------------------------
# Metadata counts
# ---------------------------------------------------------------------------

def test_kurd_available_reflects_tenant_limit():
    """available should reflect the tenant-restricted count, not total."""
    manager = TenantManager()
    manager.add_tenant("search-t", name="Searcher",
                       allowed_tools=["add", "multiply"], api_key="sk-search")
    _router.set_policy_engine(manager)
    try:
        # Without client filter: available == 2 (tenant restricted), returned == 2
        result = _list_tools(token="sk-search")
        assert result["_kurd"]["available"] == 2
        assert result["_kurd"]["returned"] == 2

        # With search filter: available stays 2, returned may be smaller
        result = _list_tools({"search": "add"}, token="sk-search")
        assert result["_kurd"]["available"] == 2
        assert result["_kurd"]["returned"] == 1
        assert result["tools"][0]["name"] == "add"
    finally:
        _router.clear_policy_engine()


def test_client_filter_cannot_bypass_tenant_restrictions():
    """A tenant limited to ['add'] cannot see 'file_read' via search."""
    manager = TenantManager()
    manager.add_tenant("bypass-t", name="Bypass Test",
                       allowed_tools=["add"], api_key="sk-bypass")
    _router.set_policy_engine(manager)
    try:
        result = _list_tools({"search": "file"}, token="sk-bypass")
        assert result["tools"] == []
        assert result["_kurd"]["available"] == 1  # tenant sees 1 tool
        assert result["_kurd"]["returned"] == 0   # search found nothing in that 1
    finally:
        _router.clear_policy_engine()


# ---------------------------------------------------------------------------
# GET /admin/tools/namespaces
# ---------------------------------------------------------------------------

def test_admin_namespaces_includes_local():
    data = _get("/admin/tools/namespaces")
    assert "namespaces" in data
    sources = [n["source"] for n in data["namespaces"]]
    assert "local" in sources


def test_admin_namespaces_structure():
    data = _get("/admin/tools/namespaces")
    assert "count" in data
    assert data["count"] == len(data["namespaces"])
    for ns in data["namespaces"]:
        assert "namespace" in ns
        assert "source" in ns
