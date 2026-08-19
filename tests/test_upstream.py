import concurrent.futures
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


KURD_URL = "http://127.0.0.1:9200/mcp"
UPSTREAM_SERVER = Path(__file__).with_name("upstream_server.py")


def post_json(
    url: str,
    payload: dict,
    headers: dict | None = None,
    *,
    allow_http_error: bool = False,
) -> dict:
    request_headers = {"content-type": "application/json"}
    if headers:
        request_headers.update(headers)

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers=request_headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        if not allow_http_error:
            raise

        body = error.read()
        parsed = json.loads(body) if body else {}
        parsed["_http_status"] = error.code
        return parsed


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read())


def wait_for_server(url: str, timeout: float = 5.0):
    started = time.perf_counter()

    while time.perf_counter() - started < timeout:
        try:
            request = urllib.request.Request(
                url,
                data=b"{}",
                headers={"content-type": "application/json"},
                method="POST",
            )

            urllib.request.urlopen(request, timeout=0.5)
            return

        except Exception:
            time.sleep(0.05)

    raise RuntimeError(f"Server did not start: {url}")


def terminate_process(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return

    process.terminate()

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def start_upstream(port: int, tool_name: str, delay: float = 0.0):
    return subprocess.Popen(
        [
            sys.executable,
            str(UPSTREAM_SERVER),
            "--port",
            str(port),
            "--tool-name",
            tool_name,
            "--delay",
            str(delay),
        ],
    )


def start_gateway(mounts: list[tuple[str, str]]):
    mount_lines = "\n".join(
        f'router.mount({name!r}, {url!r})'
        for name, url in mounts
    )

    gateway_code = f"""
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
{mount_lines}

start_http_gateway("127.0.0.1:9200")
"""

    return subprocess.Popen(
        [sys.executable, "-c", gateway_code],
    )


def test_upstream_tools_list_and_call():
    gateway = None

    upstream = start_upstream(
        port=9100,
        tool_name="add",
    )

    try:
        wait_for_server("http://127.0.0.1:9100")

        gateway = start_gateway(
            [
                ("remote", "http://127.0.0.1:9100"),
            ]
        )

        wait_for_server(KURD_URL)

        tools_response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {},
            },
        )

        names = [
            tool["name"]
            for tool in tools_response["result"]["tools"]
        ]

        assert "remote.add" in names

        call_response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "remote.add",
                    "arguments": {
                        "a": 20,
                        "b": 22,
                    },
                },
            },
        )

        assert call_response["result"]["isError"] is False
        assert call_response["result"]["content"][0]["text"] == "42"

    finally:
        if gateway is not None:
            terminate_process(gateway)
        terminate_process(upstream)


def test_upstream_tools_list_runs_concurrently():
    delay = 0.7

    upstream_one = start_upstream(
        port=9100,
        tool_name="add",
        delay=delay,
    )

    upstream_two = start_upstream(
        port=9101,
        tool_name="multiply",
        delay=delay,
    )

    gateway = None

    try:
        wait_for_server("http://127.0.0.1:9100")
        wait_for_server("http://127.0.0.1:9101")

        gateway = start_gateway(
            [
                ("alpha", "http://127.0.0.1:9100"),
                ("beta", "http://127.0.0.1:9101"),
            ]
        )

        wait_for_server(KURD_URL)

        started = time.perf_counter()

        tools_response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/list",
                "params": {},
            },
        )

        elapsed = time.perf_counter() - started

        names = {
            tool["name"]
            for tool in tools_response["result"]["tools"]
        }

        assert "alpha.add" in names
        assert "beta.multiply" in names

        # Each upstream deliberately sleeps for 0.7 s.
        # Sequential discovery would require at least ~1.4 s.
        # Concurrent discovery should stay comfortably below that.
        assert elapsed < 1.30, (
            f"tools/list took {elapsed:.3f}s; "
            "upstream discovery appears sequential"
        )

    finally:
        terminate_process(gateway)
        terminate_process(upstream_one)
        terminate_process(upstream_two)

def test_upstream_tools_list_uses_cache():
    upstream = None
    gateway = None

    try:
        upstream = start_upstream(
            port=9100,
            tool_name="add",
        )

        wait_for_server("http://127.0.0.1:9100")

        gateway = start_gateway(
            [
                ("remote", "http://127.0.0.1:9100"),
            ]
        )

        wait_for_server(KURD_URL)

        first = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 20,
                "method": "tools/list",
                "params": {},
            },
        )

        first_names = {
            tool["name"]
            for tool in first["result"]["tools"]
        }

        assert "remote.add" in first_names

        counter_after_first = post_json(
            "http://127.0.0.1:9100",
            {
                "jsonrpc": "2.0",
                "id": 21,
                "method": "test/counter",
                "params": {},
            },
        )

        assert counter_after_first["result"]["toolsListCalls"] == 1

        second = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 22,
                "method": "tools/list",
                "params": {},
            },
        )

        second_names = {
            tool["name"]
            for tool in second["result"]["tools"]
        }

        assert "remote.add" in second_names

        counter_after_second = post_json(
            "http://127.0.0.1:9100",
            {
                "jsonrpc": "2.0",
                "id": 23,
                "method": "test/counter",
                "params": {},
            },
        )

        assert counter_after_second["result"]["toolsListCalls"] == 1

    finally:
        terminate_process(gateway)
        terminate_process(upstream)

def test_router_unmount_removes_upstream():
    upstream = None
    gateway = None

    try:
        upstream = start_upstream(
            port=9100,
            tool_name="add",
        )

        wait_for_server("http://127.0.0.1:9100")

        gateway_code = """
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
router.mount("remote", "http://127.0.0.1:9100")
removed = router.unmount("remote")
assert removed is True

start_http_gateway("127.0.0.1:9200")
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        tools_response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 30,
                "method": "tools/list",
                "params": {},
            },
        )

        names = {
            tool["name"]
            for tool in tools_response["result"]["tools"]
        }

        assert "remote.add" not in names

        status = get_json("http://127.0.0.1:9200/status")

        assert status["upstreamCount"] == 0
        assert "remote" not in status["upstreams"]

    finally:
        terminate_process(gateway)
        terminate_process(upstream)


def test_router_refresh_tools_invalidates_cache():
    upstream = None
    gateway = None

    try:
        upstream = start_upstream(
            port=9100,
            tool_name="add",
        )

        wait_for_server("http://127.0.0.1:9100")

        gateway_code = """
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
router.mount("remote", "http://127.0.0.1:9100")

import threading
import time

def refresh_later():
    time.sleep(1.0)
    router.refresh_tools()

threading.Thread(target=refresh_later, daemon=True).start()

start_http_gateway("127.0.0.1:9200")
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 40,
                "method": "tools/list",
                "params": {},
            },
        )

        counter_after_first = post_json(
            "http://127.0.0.1:9100",
            {
                "jsonrpc": "2.0",
                "id": 41,
                "method": "test/counter",
                "params": {},
            },
        )

        assert counter_after_first["result"]["toolsListCalls"] == 1

        time.sleep(1.2)

        post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 42,
                "method": "tools/list",
                "params": {},
            },
        )

        counter_after_refresh = post_json(
            "http://127.0.0.1:9100",
            {
                "jsonrpc": "2.0",
                "id": 43,
                "method": "test/counter",
                "params": {},
            },
        )

        assert counter_after_refresh["result"]["toolsListCalls"] == 2

    finally:
        terminate_process(gateway)
        terminate_process(upstream)


def test_status_reports_tools_cache_metrics():
    upstream = None
    gateway = None

    try:
        upstream = start_upstream(
            port=9100,
            tool_name="add",
        )

        wait_for_server("http://127.0.0.1:9100")

        gateway = start_gateway(
            [
                ("remote", "http://127.0.0.1:9100"),
            ]
        )

        wait_for_server(KURD_URL)

        status_before = get_json("http://127.0.0.1:9200/status")
        before = status_before["toolsCache"]

        post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 50,
                "method": "tools/list",
                "params": {},
            },
        )

        post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 51,
                "method": "tools/list",
                "params": {},
            },
        )

        status_after = get_json("http://127.0.0.1:9200/status")
        after = status_after["toolsCache"]

        assert after["cached"] is True
        assert after["toolCount"] >= 1
        assert after["misses"] >= before["misses"] + 1
        assert after["hits"] >= before["hits"] + 1
        assert after["invalidations"] >= before["invalidations"]

    finally:
        terminate_process(gateway)
        terminate_process(upstream)

def modern_headers(method: str, name: str | None = None) -> dict:
    headers = {
        "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": method,
    }
    if name is not None:
        headers["Mcp-Name"] = name
    return headers


def modern_meta() -> dict:
    return {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {
            "name": "kurd-test-client",
            "version": "1.0.0",
        },
        "io.modelcontextprotocol/clientCapabilities": {},
    }


def test_modern_tools_list_response_metadata():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 60,
                "method": "tools/list",
                "params": {
                    "_meta": modern_meta(),
                },
            },
            headers=modern_headers("tools/list"),
        )

        result = response["result"]

        assert result["resultType"] == "complete"
        assert isinstance(result["tools"], list)
        assert result["ttlMs"] == 30000
        assert result["cacheScope"] == "public"

    finally:
        terminate_process(gateway)


def test_modern_tools_call_response_has_result_type():
    gateway = None

    try:
        gateway_code = """
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()

@router.tool()
def add(a: int, b: int) -> int:
    return a + b

start_http_gateway("127.0.0.1:9200")
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 61,
                "method": "tools/call",
                "params": {
                    "name": "add",
                    "arguments": {
                        "a": 20,
                        "b": 22,
                    },
                    "_meta": modern_meta(),
                },
            },
            headers=modern_headers("tools/call", "add"),
        )

        assert response["result"]["resultType"] == "complete"
        assert response["result"]["isError"] is False
        assert response["result"]["content"][0]["text"] == "42"

    finally:
        terminate_process(gateway)


def test_modern_request_rejects_mcp_method_mismatch():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 62,
                "method": "tools/list",
                "params": {
                    "_meta": modern_meta(),
                },
            },
            headers=modern_headers("tools/call"),
            allow_http_error=True,
        )

        assert response["_http_status"] == 400
        assert response["error"]["code"] == -32020
        assert "Mcp-Method" in response["error"]["message"]

    finally:
        terminate_process(gateway)


def test_modern_tools_call_rejects_mcp_name_mismatch():
    gateway = None

    try:
        gateway_code = """
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()

@router.tool()
def add(a: int, b: int) -> int:
    return a + b

start_http_gateway("127.0.0.1:9200")
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 63,
                "method": "tools/call",
                "params": {
                    "name": "add",
                    "arguments": {
                        "a": 1,
                        "b": 2,
                    },
                    "_meta": modern_meta(),
                },
            },
            headers=modern_headers("tools/call", "wrong-name"),
            allow_http_error=True,
        )

        assert response["_http_status"] == 400
        assert response["error"]["code"] == -32020
        assert "Mcp-Name" in response["error"]["message"]

    finally:
        terminate_process(gateway)


def test_modern_tools_call_requires_mcp_name_header():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 64,
                "method": "tools/call",
                "params": {
                    "name": "missing",
                    "arguments": {},
                    "_meta": modern_meta(),
                },
            },
            headers=modern_headers("tools/call"),
            allow_http_error=True,
        )

        assert response["_http_status"] == 400
        assert response["error"]["code"] == -32020
        assert "Mcp-Name" in response["error"]["message"]

    finally:
        terminate_process(gateway)


def test_modern_request_rejects_unsupported_protocol_version():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 65,
                "method": "tools/list",
                "params": {
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2099-01-01",
                        "io.modelcontextprotocol/clientInfo": {
                            "name": "kurd-test-client",
                            "version": "1.0.0",
                        },
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                },
            },
            headers={
                "MCP-Protocol-Version": "2099-01-01",
                "Mcp-Method": "tools/list",
            },
            allow_http_error=True,
        )

        assert response["_http_status"] == 400
        assert response["error"]["code"] == -32022
        assert response["error"]["data"]["requested"] == "2099-01-01"
        assert response["error"]["data"]["supported"] == ["2026-07-28"]

    finally:
        terminate_process(gateway)


def test_modern_request_requires_mcp_method_header():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 66,
                "method": "tools/list",
                "params": {
                    "_meta": modern_meta(),
                },
            },
            headers={
                "MCP-Protocol-Version": "2026-07-28",
            },
            allow_http_error=True,
        )

        assert response["_http_status"] == 400
        assert response["error"]["code"] == -32020
        assert "Mcp-Method" in response["error"]["message"]

    finally:
        terminate_process(gateway)

def test_http_gateway_status_reports_running_and_address():
    gateway = None

    try:
        gateway_code = """
import threading
import time

from kurd import Router
from kurd._kurd import (
    http_gateway_status,
    start_http_gateway,
)

router = Router()

def run_server():
    start_http_gateway("127.0.0.1:9200")

thread = threading.Thread(target=run_server, daemon=True)
thread.start()

deadline = time.time() + 5
while time.time() < deadline:
    running, address = http_gateway_status()
    if running:
        assert address == "127.0.0.1:9200"
        break
    time.sleep(0.05)
else:
    raise RuntimeError("gateway never reported running")

thread.join()
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        status = get_json("http://127.0.0.1:9200/status")
        assert status["http"]["running"] is True
        assert status["http"]["address"] == "127.0.0.1:9200"

    finally:
        terminate_process(gateway)


def test_http_gateway_stop_and_restart():
    gateway = None

    try:
        gateway_code = """
import threading
import time

from kurd import Router
from kurd._kurd import (
    http_gateway_status,
    start_http_gateway,
    stop_http_gateway,
)

router = Router()

def run_server():
    start_http_gateway("127.0.0.1:9200")

first = threading.Thread(target=run_server)
first.start()

deadline = time.time() + 5
while time.time() < deadline:
    running, _ = http_gateway_status()
    if running:
        break
    time.sleep(0.05)
else:
    raise RuntimeError("first start did not become ready")

assert stop_http_gateway() is True
first.join(timeout=5)
assert not first.is_alive()

running, address = http_gateway_status()
assert running is False
assert address is None

second = threading.Thread(target=run_server)
second.start()

deadline = time.time() + 5
while time.time() < deadline:
    running, address = http_gateway_status()
    if running:
        assert address == "127.0.0.1:9200"
        break
    time.sleep(0.05)
else:
    raise RuntimeError("restart did not become ready")

assert stop_http_gateway() is True
second.join(timeout=5)
assert not second.is_alive()
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        gateway.wait(timeout=10)
        assert gateway.returncode == 0

    finally:
        terminate_process(gateway)


def test_http_gateway_double_start_is_rejected():
    process = None

    try:
        gateway_code = """
import threading
import time

from kurd import Router
from kurd._kurd import (
    http_gateway_status,
    start_http_gateway,
    stop_http_gateway,
)

router = Router()

first_error = []
second_error = []

def first_server():
    try:
        start_http_gateway("127.0.0.1:9200")
    except Exception as exc:
        first_error.append(str(exc))

first = threading.Thread(target=first_server)
first.start()

deadline = time.time() + 5
while time.time() < deadline:
    running, _ = http_gateway_status()
    if running:
        break
    time.sleep(0.05)
else:
    raise RuntimeError("first server did not become ready")

try:
    start_http_gateway("127.0.0.1:9201")
except Exception as exc:
    second_error.append(str(exc))

assert second_error
assert "already running" in second_error[0]

assert stop_http_gateway() is True
first.join(timeout=5)
assert not first.is_alive()
assert not first_error
"""

        process = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        process.wait(timeout=10)
        assert process.returncode == 0

    finally:
        terminate_process(process)


def test_http_gateway_stop_when_not_running_returns_false():
    process = None

    try:
        gateway_code = """
from kurd._kurd import http_gateway_status, stop_http_gateway

running, address = http_gateway_status()
assert running is False
assert address is None
assert stop_http_gateway() is False
"""

        process = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        process.wait(timeout=5)
        assert process.returncode == 0

    finally:
        terminate_process(process)

def raw_post(
    url: str,
    body: bytes,
    headers: dict | None = None,
    *,
    allow_http_error: bool = False,
) -> tuple[int, bytes]:
    request = urllib.request.Request(
        url,
        data=body,
        headers=headers or {},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        if not allow_http_error:
            raise
        return error.code, error.read()


def test_security_rejects_invalid_content_type():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        status, body = raw_post(
            KURD_URL,
            json.dumps({
                "jsonrpc": "2.0",
                "id": 70,
                "method": "tools/list",
                "params": {},
            }).encode(),
            headers={"content-type": "text/plain"},
            allow_http_error=True,
        )

        response = json.loads(body)

        assert status == 415
        assert response["error"]["code"] == -32600
        assert "Content-Type" in response["error"]["message"]

    finally:
        terminate_process(gateway)


def test_security_rejects_oversized_request_body():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        oversized = b'{"jsonrpc":"2.0","id":71,"method":"tools/list","params":{"blob":"' + (
            b"x" * (1024 * 1024)
        ) + b'"}}'

        status, body = raw_post(
            KURD_URL,
            oversized,
            headers={"content-type": "application/json"},
            allow_http_error=True,
        )

        assert status == 413

        # Axum's body-limit layer may reject before the handler is entered,
        # so the response body is intentionally not required to be JSON here.
        assert body is not None

    finally:
        terminate_process(gateway)


def test_security_bearer_auth_rejects_missing_and_wrong_token():
    process = None

    try:
        gateway_code = """
from kurd import Router
from kurd._kurd import set_http_bearer_token, start_http_gateway

router = Router()
set_http_bearer_token("secret-token")
start_http_gateway("127.0.0.1:9200")
"""

        process = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        deadline = time.perf_counter() + 5
        while time.perf_counter() < deadline:
            try:
                probe = post_json(
                    KURD_URL,
                    {
                        "jsonrpc": "2.0",
                        "id": 720,
                        "method": "tools/list",
                        "params": {},
                    },
                    headers={"Authorization": "Bearer secret-token"},
                )
                if "result" in probe:
                    break
            except Exception:
                pass
            time.sleep(0.05)
        else:
            raise RuntimeError("authenticated gateway did not become ready")

        missing = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 72,
                "method": "tools/list",
                "params": {},
            },
            allow_http_error=True,
        )

        wrong = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 73,
                "method": "tools/list",
                "params": {},
            },
            headers={"Authorization": "Bearer wrong-token"},
            allow_http_error=True,
        )

        assert missing["_http_status"] == 401
        assert missing["error"]["code"] == -32001
        assert wrong["_http_status"] == 401
        assert wrong["error"]["code"] == -32001

    finally:
        terminate_process(process)


def test_security_bearer_auth_accepts_correct_token():
    process = None

    try:
        gateway_code = """
from kurd import Router
from kurd._kurd import set_http_bearer_token, start_http_gateway

router = Router()
set_http_bearer_token("secret-token")
start_http_gateway("127.0.0.1:9200")
"""

        process = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        # The normal readiness helper cannot authenticate, so poll manually.
        deadline = time.perf_counter() + 5
        while time.perf_counter() < deadline:
            try:
                response = post_json(
                    KURD_URL,
                    {
                        "jsonrpc": "2.0",
                        "id": 74,
                        "method": "tools/list",
                        "params": {},
                    },
                    headers={"Authorization": "Bearer secret-token"},
                )
                assert "result" in response
                break
            except Exception:
                time.sleep(0.05)
        else:
            raise RuntimeError("authenticated gateway did not become ready")

    finally:
        terminate_process(process)


def test_security_upstream_url_validation():
    process = None

    try:
        code = """
from kurd._kurd import register_upstream

def expect_invalid(name, url):
    try:
        register_upstream(name, url)
    except ValueError:
        return
    raise AssertionError(f"expected invalid upstream URL: {url}")

expect_invalid("ftp", "ftp://example.com/mcp")
expect_invalid("creds", "http://user:pass@example.com/mcp")
expect_invalid("fragment", "https://example.com/mcp#secret")
expect_invalid("missing-host", "http://")
"""

        process = subprocess.Popen(
            [sys.executable, "-c", code],
        )

        process.wait(timeout=5)
        assert process.returncode == 0

    finally:
        terminate_process(process)


def test_security_private_upstream_policy():
    process = None

    try:
        code = """
from kurd._kurd import register_upstream, set_allow_private_upstreams

set_allow_private_upstreams(False)

for name, url in [
    ("loopback", "http://127.0.0.1:9100"),
    ("localhost", "http://localhost:9100"),
    ("private", "http://10.0.0.1:9100"),
]:
    try:
        register_upstream(name, url)
    except ValueError:
        pass
    else:
        raise AssertionError(f"private upstream unexpectedly allowed: {url}")

set_allow_private_upstreams(True)
register_upstream("allowed", "http://127.0.0.1:9100")
"""

        process = subprocess.Popen(
            [sys.executable, "-c", code],
        )

        process.wait(timeout=5)
        assert process.returncode == 0

    finally:
        terminate_process(process)


def test_security_upstream_timeout_configuration():
    process = None

    try:
        code = """
from kurd._kurd import security_status, set_upstream_timeout_ms

set_upstream_timeout_ms(2500)
auth_enabled, allow_private, timeout_ms, max_body = security_status()

assert timeout_ms == 2500
assert max_body == 1024 * 1024

try:
    set_upstream_timeout_ms(0)
except ValueError:
    pass
else:
    raise AssertionError("zero timeout should be rejected")
"""

        process = subprocess.Popen(
            [sys.executable, "-c", code],
        )

        process.wait(timeout=5)
        assert process.returncode == 0

    finally:
        terminate_process(process)


def test_status_reports_security_configuration():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        status = get_json("http://127.0.0.1:9200/status")
        security = status["security"]

        assert security["authEnabled"] is False
        assert security["maxMcpBodyBytes"] == 1024 * 1024
        assert security["allowPrivateUpstreams"] is True
        assert security["upstreamTimeoutMs"] == 30000

    finally:
        terminate_process(gateway)


def test_security_upstream_error_does_not_leak_internal_details():
    gateway = None

    try:
        gateway_code = """
from kurd import Router
from kurd._kurd import set_upstream_timeout_ms, start_http_gateway

router = Router()
set_upstream_timeout_ms(250)
router.mount("dead", "http://127.0.0.1:65530")

start_http_gateway("127.0.0.1:9200")
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        response = post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 75,
                "method": "tools/call",
                "params": {
                    "name": "dead.missing",
                    "arguments": {},
                },
            },
        )

        assert response["error"]["code"] == -32000
        assert response["error"]["message"] == "Upstream request failed"
        assert response["error"]["data"] == {
            "upstream": "dead",
            "reason": "request_failed",
        }

        serialized = json.dumps(response).lower()
        assert "connection refused" not in serialized
        assert "os error" not in serialized

    finally:
        terminate_process(gateway)

def post_json_with_response_headers(
    url: str,
    payload: dict,
    headers: dict | None = None,
    *,
    allow_http_error: bool = False,
) -> tuple[int, dict, dict]:
    request_headers = {"content-type": "application/json"}
    if headers:
        request_headers.update(headers)

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers=request_headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return (
                response.status,
                json.loads(response.read()),
                dict(response.headers.items()),
            )
    except urllib.error.HTTPError as error:
        if not allow_http_error:
            raise

        body = error.read()
        parsed = json.loads(body) if body else {}
        return error.code, parsed, dict(error.headers.items())


def test_runtime_configuration_api_reports_limits():
    process = None

    try:
        code = """
from kurd import Router

router = Router()
router.configure_runtime(
    global_concurrency=17,
    upstream_concurrency=7,
    python_concurrency=5,
    request_logging=True,
)

status = router.runtime_status()

assert status["globalConcurrencyLimit"] == 17
assert status["upstreamConcurrencyLimit"] == 7
assert status["pythonConcurrencyLimit"] == 5
assert status["requestLoggingEnabled"] is True
assert status["activeRequests"] == 0
"""

        process = subprocess.Popen(
            [sys.executable, "-c", code],
        )

        process.wait(timeout=5)
        assert process.returncode == 0

    finally:
        terminate_process(process)


def test_status_reports_runtime_observability_metrics():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 80,
                "method": "tools/list",
                "params": {},
            },
        )

        status = get_json("http://127.0.0.1:9200/status")
        runtime = status["runtime"]

        assert runtime["globalConcurrencyLimit"] == 512
        assert runtime["upstreamConcurrencyLimit"] == 64
        assert runtime["pythonConcurrencyLimit"] == 64
        assert runtime["activeRequests"] == 0
        assert runtime["peakActiveRequests"] >= 1
        assert runtime["totalRequests"] >= 2
        assert runtime["completedRequests"] >= 2
        assert runtime["rejectedRequests"] >= 0
        assert runtime["averageLatencyMs"] >= 0
        assert runtime["pythonActiveCalls"] == 0
        assert runtime["pythonPeakActiveCalls"] >= 0
        assert runtime["pythonRejectedCalls"] >= 0
        assert runtime["upstreamRejectedCalls"] >= 0
        assert runtime["requestLoggingEnabled"] is False
        assert isinstance(runtime["upstreams"], dict)

    finally:
        terminate_process(gateway)


def test_request_id_header_is_preserved():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        status, response, headers = post_json_with_response_headers(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 81,
                "method": "tools/list",
                "params": {},
            },
            headers={"X-Request-ID": "external-request-123"},
        )

        assert status == 200
        assert "result" in response

        normalized = {key.lower(): value for key, value in headers.items()}
        assert normalized["x-request-id"] == "external-request-123"

    finally:
        terminate_process(gateway)


def test_request_id_header_is_generated_when_missing():
    gateway = None

    try:
        gateway = start_gateway([])
        wait_for_server(KURD_URL)

        status, response, headers = post_json_with_response_headers(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": 82,
                "method": "tools/list",
                "params": {},
            },
        )

        assert status == 200
        assert "result" in response

        normalized = {key.lower(): value for key, value in headers.items()}
        request_id = normalized["x-request-id"]

        assert request_id.startswith("kurd-")
        assert len(request_id) > len("kurd-")

    finally:
        terminate_process(gateway)


def test_global_backpressure_rejects_excess_concurrency():
    gateway = None

    try:
        gateway_code = """
import asyncio

from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
router.configure_runtime(
    global_concurrency=1,
    upstream_concurrency=8,
    python_concurrency=8,
)

@router.tool()
async def slow(value: int) -> int:
    await asyncio.sleep(0.7)
    return value

start_http_gateway("127.0.0.1:9200")
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        def call(request_id: int, value: int, allow_error: bool = False):
            return post_json(
                KURD_URL,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                    "params": {
                        "name": "slow",
                        "arguments": {"value": value},
                    },
                },
                allow_http_error=allow_error,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(call, 83, 1)
            time.sleep(0.15)

            second = call(84, 2, allow_error=True)
            first_response = first.result(timeout=5)

        assert first_response["result"]["isError"] is False

        assert second["_http_status"] == 503
        assert second["error"]["code"] == -32029
        assert second["error"]["message"] == "Gateway overloaded"
        assert second["error"]["data"]["limit"] == 1

        status = get_json("http://127.0.0.1:9200/status")
        runtime = status["runtime"]

        assert runtime["globalConcurrencyLimit"] == 1
        assert runtime["peakActiveRequests"] == 1
        assert runtime["rejectedRequests"] >= 1

    finally:
        terminate_process(gateway)


def test_python_callback_backpressure_rejects_excess_calls():
    gateway = None

    try:
        gateway_code = """
import asyncio

from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
router.configure_runtime(
    global_concurrency=8,
    upstream_concurrency=8,
    python_concurrency=1,
)

@router.tool()
async def slow(value: int) -> int:
    await asyncio.sleep(0.7)
    return value

start_http_gateway("127.0.0.1:9200")
"""

        gateway = subprocess.Popen(
            [sys.executable, "-c", gateway_code],
        )

        wait_for_server(KURD_URL)

        def call(request_id: int, value: int, allow_error: bool = False):
            return post_json(
                KURD_URL,
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                    "params": {
                        "name": "slow",
                        "arguments": {"value": value},
                    },
                },
                allow_http_error=allow_error,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(call, 85, 1)
            time.sleep(0.15)

            second = call(86, 2, allow_error=True)
            first_response = first.result(timeout=5)

        assert first_response["result"]["isError"] is False

        assert second["_http_status"] == 503
        assert second["error"]["code"] == -32029
        assert second["error"]["message"] == "Python tool executor overloaded"
        assert second["error"]["data"]["limit"] == 1

        status = get_json("http://127.0.0.1:9200/status")
        runtime = status["runtime"]

        assert runtime["pythonConcurrencyLimit"] == 1
        assert runtime["pythonPeakActiveCalls"] == 1
        assert runtime["pythonRejectedCalls"] >= 1

    finally:
        terminate_process(gateway)


def test_runtime_rejects_zero_concurrency_limits():
    process = None

    try:
        code = """
from kurd import Router

router = Router()

for kwargs in [
    {"global_concurrency": 0},
    {"upstream_concurrency": 0},
    {"python_concurrency": 0},
]:
    values = {
        "global_concurrency": 8,
        "upstream_concurrency": 8,
        "python_concurrency": 8,
    }
    values.update(kwargs)

    try:
        router.configure_runtime(**values)
    except ValueError:
        pass
    else:
        raise AssertionError(f"expected invalid concurrency config: {values}")
"""

        process = subprocess.Popen(
            [sys.executable, "-c", code],
        )

        process.wait(timeout=5)
        assert process.returncode == 0

    finally:
        terminate_process(process)

