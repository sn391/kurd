"""
P2 tests: per-tenant tools/list filtering.

A TenantManager is wired via set_policy_engine(). tools/list must return
only the tools the caller is allowed to see, based on their API key.
"""
import json
import threading
import time
import urllib.request

import pytest

from kurd import Router, TenantManager
from kurd._kurd import start_http_gateway, stop_http_gateway

_PORT = 18733
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


def _list_tools(token: str | None = None) -> list[str]:
    result = _mcp({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, token=token)
    assert "result" in result, result
    return [t["name"] for t in result["result"]["tools"]]


@pytest.fixture(scope="module", autouse=True)
def gateway():
    global _router
    _router = Router()

    @_router.tool(name="add")
    async def add(a: int, b: int) -> int:
        return a + b

    @_router.tool(name="multiply")
    async def multiply(a: int, b: int) -> int:
        return a * b

    @_router.tool(name="divide")
    async def divide(a: int, b: int) -> float:
        return a / b

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


def test_no_filter_shows_all_tools():
    names = _list_tools()
    assert "add" in names
    assert "multiply" in names
    assert "divide" in names


def test_tenant_with_wildcard_sees_all_tools():
    manager = TenantManager()
    manager.add_tenant("admin", name="Admin", allowed_tools=["*"], api_key="sk-admin")
    _router.set_policy_engine(manager)
    try:
        names = _list_tools(token="sk-admin")
        assert "add" in names
        assert "multiply" in names
        assert "divide" in names
    finally:
        _router.clear_policy_engine()


def test_tenant_restricted_to_one_tool():
    manager = TenantManager()
    manager.add_tenant("t1", name="Restricted", allowed_tools=["add"], api_key="sk-add-only")
    _router.set_policy_engine(manager)
    try:
        names = _list_tools(token="sk-add-only")
        assert "add" in names
        assert "multiply" not in names
        assert "divide" not in names
    finally:
        _router.clear_policy_engine()


def test_tenant_restricted_to_two_tools():
    manager = TenantManager()
    manager.add_tenant("t2", name="Two Tools", allowed_tools=["add", "multiply"], api_key="sk-two")
    _router.set_policy_engine(manager)
    try:
        names = _list_tools(token="sk-two")
        assert "add" in names
        assert "multiply" in names
        assert "divide" not in names
    finally:
        _router.clear_policy_engine()


def test_unknown_api_key_sees_no_tools():
    manager = TenantManager()
    manager.add_tenant("t3", name="Other", allowed_tools=["add"], api_key="sk-other")
    _router.set_policy_engine(manager)
    try:
        # Unknown key → filter callback returns [] → no tools visible
        names = _list_tools(token="sk-unknown")
        assert names == []
    finally:
        _router.clear_policy_engine()


def test_clear_policy_engine_restores_full_list():
    manager = TenantManager()
    manager.add_tenant("t4", name="Single", allowed_tools=["add"], api_key="sk-single")
    _router.set_policy_engine(manager)
    _router.clear_policy_engine()
    # After clearing, all tools are visible again regardless of token
    names = _list_tools(token="sk-single")
    assert "add" in names
    assert "multiply" in names
    assert "divide" in names


def test_filter_consistent_with_policy_for_tools_call():
    """A tenant that can only see 'add' in tools/list should also be blocked
    from calling 'multiply' via tools/call (P0 policy gate)."""
    import urllib.error
    manager = TenantManager()
    manager.add_tenant("t5", name="Add Only", allowed_tools=["add"], api_key="sk-add-call")
    _router.set_policy_engine(manager)
    try:
        # Can see add
        names = _list_tools(token="sk-add-call")
        assert "add" in names
        assert "multiply" not in names

        # Can call add
        result = _mcp(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
             "params": {"name": "add", "arguments": {"a": 1, "b": 2}}},
            token="sk-add-call",
        )
        assert "result" in result

        # Cannot call multiply
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _mcp(
                {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                 "params": {"name": "multiply", "arguments": {"a": 2, "b": 3}}},
                token="sk-add-call",
            )
        assert exc_info.value.code == 403
    finally:
        _router.clear_policy_engine()
