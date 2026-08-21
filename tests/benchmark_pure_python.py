"""
Pure Python MCP Gateway - Benchmark Baseline

This is a simplified MCP gateway implemented entirely in Python using asyncio + FastAPI.
It provides the same features as Kurd without the Rust backend, serving as a performance baseline.

Features:
  - Local Python tools
  - Upstream MCP server mounting
  - Tool discovery aggregation
  - Basic caching (no TTL, simple dict)
  - Basic retry (no circuit breaker)
  - Concurrency control via semaphore
  - Request ID generation

Run this with pytest to benchmark against Kurd.
"""

import asyncio
import concurrent.futures
import json
import math
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Dict, Any


# ============================================================================
# Pure Python Gateway Implementation
# ============================================================================

class PurePythonMCPGateway:
    """Minimal MCP gateway in pure Python using asyncio."""

    def __init__(self, host: str = "127.0.0.1", port: int = 9201):
        self.host = host
        self.port = port
        self.tools: Dict[str, Callable] = {}
        self.upstreams: Dict[str, str] = {}
        self.tool_cache: Dict[str, list] = {}
        self.semaphore = asyncio.Semaphore(512)  # Global concurrency
        self.request_count = 0
        self.retry_count = 3
        self.retry_delay = 0.1

    def register_tool(self, name: str = None, func: Callable = None):
        """Register a local tool - works as decorator or direct call."""
        if func is None:
            # Used as decorator: @gateway.register_tool("add")
            def decorator(f):
                tool_name = name or f.__name__
                self.tools[tool_name] = f
                return f
            return decorator
        else:
            # Direct call: gateway.register_tool("add", func)
            self.tools[name] = func

    def mount_upstream(self, name: str, url: str) -> None:
        """Mount an upstream MCP server."""
        self.upstreams[name] = url
        if name in self.tool_cache:
            del self.tool_cache[name]

    async def discover_upstream_tools(self, name: str) -> list[dict]:
        """Discover tools from upstream MCP server."""
        if name in self.tool_cache:
            return self.tool_cache[name]

        url = self.upstreams.get(name)
        if not url:
            return []

        try:
            # Call tools/list on upstream
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {},
            }

            response = await self._http_post(url, payload)
            tools = response.get("result", {}).get("tools", [])

            # Namespace tools with upstream name
            namespaced = [
                {**tool, "name": f"{name}.{tool['name']}"}
                for tool in tools
            ]

            self.tool_cache[name] = namespaced
            return namespaced

        except Exception as e:
            print(f"Failed to discover tools from {name}: {e}")
            return []

    async def list_tools(self) -> list[dict]:
        """List all available tools (local + upstream)."""
        all_tools = []

        # Add local tools
        for name in self.tools:
            all_tools.append({
                "name": name,
                "description": "Local Python tool",
                "inputSchema": {"type": "object", "properties": {}},
            })

        # Add upstream tools
        for upstream_name in self.upstreams:
            upstream_tools = await self.discover_upstream_tools(upstream_name)
            all_tools.extend(upstream_tools)

        return all_tools

    async def call_tool(self, name: str, arguments: dict) -> Any:
        """Call a tool (local or upstream)."""
        if name in self.tools:
            return await self._call_local_tool(name, arguments)

        if "." in name:
            upstream_name, tool_name = name.split(".", 1)
            return await self._call_upstream_tool(upstream_name, tool_name, arguments)

        raise ValueError(f"Tool not found: {name}")

    async def _call_local_tool(self, name: str, arguments: dict) -> Any:
        """Call a local tool with concurrency control."""
        async with self.semaphore:
            func = self.tools[name]

            # Check if function is async
            if asyncio.iscoroutinefunction(func):
                result = await func(**arguments)
            else:
                # Run sync function in executor to avoid blocking
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, lambda: func(**arguments))

            return result

    async def _call_upstream_tool(
        self,
        upstream_name: str,
        tool_name: str,
        arguments: dict
    ) -> Any:
        """Call an upstream tool with retry."""
        url = self.upstreams.get(upstream_name)
        if not url:
            raise ValueError(f"Upstream not found: {upstream_name}")

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            },
        }

        # Retry logic
        for attempt in range(self.retry_count):
            try:
                async with self.semaphore:
                    response = await self._http_post(url, payload)
                    return response.get("result")
            except Exception as e:
                if attempt < self.retry_count - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise

    async def _http_post(self, url: str, payload: dict) -> dict:
        """Make async HTTP POST request."""
        loop = asyncio.get_event_loop()

        def blocking_request():
            request = urllib.request.Request(
                url,
                data=json.dumps(payload).encode(),
                headers={"content-type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                return json.loads(response.read())

        return await loop.run_in_executor(None, blocking_request)

    async def dispatch(self, raw_json_rpc_payload: str) -> str:
        """Dispatch JSON-RPC request."""
        self.request_count += 1
        req_id = None

        try:
            payload = json.loads(raw_json_rpc_payload)
            method = payload.get("method")
            params = payload.get("params", {})
            req_id = payload.get("id")

            if method == "tools/list":
                tools = await self.list_tools()
                return json.dumps({
                    "jsonrpc": "2.0",
                    "result": {"tools": tools},
                    "id": req_id,
                })

            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await self.call_tool(tool_name, arguments)
                return json.dumps({
                    "jsonrpc": "2.0",
                    "result": {"value": result, "isError": False},
                    "id": req_id,
                })

            else:
                return json.dumps({
                    "jsonrpc": "2.0",
                    "error": {"code": -32601, "message": "Method not found"},
                    "id": req_id,
                })

        except Exception as e:
            return json.dumps({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": str(e),
                },
                "id": req_id,
            })


# ============================================================================
# FastAPI Server Wrapper
# ============================================================================

def start_pure_python_gateway(port: int = 9201) -> None:
    """Start the pure Python gateway as a subprocess."""
    try:
        from fastapi import FastAPI
        import uvicorn
    except ImportError:
        raise RuntimeError(
            "FastAPI and uvicorn required for pure Python benchmark. "
            "Install with: pip install fastapi uvicorn"
        )

    app = FastAPI()
    gateway = PurePythonMCPGateway(port=port)

    # Register sample tool directly
    async def add(a: int, b: int) -> int:
        await asyncio.sleep(0)  # Allow other tasks to run
        return a + b

    gateway.register_tool("add", add)

    @app.post("/mcp")
    async def mcp_endpoint(request: dict):
        payload = json.dumps(request)
        response = await gateway.dispatch(payload)
        return json.loads(response)

    @app.get("/status")
    async def status():
        return {
            "status": "ok",
            "requests": gateway.request_count,
            "tools": list(gateway.tools.keys()),
            "upstreams": list(gateway.upstreams.keys()),
        }

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="error")


# ============================================================================
# Benchmarking Utilities
# ============================================================================

UPSTREAM_SERVER = Path(__file__).with_name("upstream_server.py")
PURE_PYTHON_URL = "http://127.0.0.1:9201/mcp"
KURD_URL = "http://127.0.0.1:9200/mcp"


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(
        0,
        min(
            len(ordered) - 1,
            math.ceil((percentile_value / 100.0) * len(ordered)) - 1,
        ),
    )
    return ordered[rank]


def post_json(
    url: str,
    payload: dict,
    *,
    timeout: float = 10.0,
) -> tuple[int, dict, float]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )

    started = time.perf_counter()

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return response.status, json.loads(body), elapsed_ms
    except urllib.error.HTTPError as error:
        body = error.read()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        parsed = json.loads(body) if body else {}
        return error.code, parsed, elapsed_ms


def wait_for_server(url: str, timeout: float = 5.0) -> None:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
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


def start_pure_python_gateway_process() -> subprocess.Popen:
    code = f"""
import sys
sys.path.insert(0, {str(Path(__file__).parent)!r})

from benchmark_pure_python import start_pure_python_gateway
start_pure_python_gateway(port=9201)
"""
    return subprocess.Popen([sys.executable, "-c", code])


def start_upstream(port: int, tool_name: str = "add", call_delay: float = 0.0) -> subprocess.Popen:
    return subprocess.Popen(
        [
            sys.executable,
            str(UPSTREAM_SERVER),
            "--port",
            str(port),
            "--tool-name",
            tool_name,
            "--call-delay",
            str(call_delay),
        ],
    )


def benchmark(
    url: str,
    *,
    tool_name: str,
    concurrency: int,
    total_requests: int,
) -> dict:
    """Run benchmark against a gateway."""
    latencies = []
    successes = 0
    failures = 0
    overloads = 0

    def one_call(index: int):
        return post_json(
            url,
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": {"a": index, "b": 1},
                },
            },
        )

    started = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(one_call, index)
            for index in range(total_requests)
        ]

        for future in concurrent.futures.as_completed(futures):
            status, response, latency_ms = future.result(timeout=15)
            latencies.append(latency_ms)

            if status == 200 and response.get("result", {}).get("isError") is False:
                successes += 1
            else:
                failures += 1
                if status == 503:
                    overloads += 1

    elapsed = time.perf_counter() - started

    return {
        "concurrency": concurrency,
        "requests": total_requests,
        "successes": successes,
        "failures": failures,
        "overloads": overloads,
        "error_rate": failures / total_requests,
        "elapsed_s": elapsed,
        "throughput_rps": total_requests / elapsed,
        "latency_mean_ms": statistics.fmean(latencies),
        "latency_p50_ms": percentile(latencies, 50),
        "latency_p95_ms": percentile(latencies, 95),
        "latency_p99_ms": percentile(latencies, 99),
        "latency_max_ms": max(latencies),
    }


def print_result(label: str, result: dict) -> None:
    print(
        f"\n[{label}] "
        f"concurrency={result['concurrency']} "
        f"requests={result['requests']} "
        f"success={result['successes']} "
        f"failures={result['failures']} "
        f"error_rate={result['error_rate']:.2%} "
        f"throughput={result['throughput_rps']:.1f} req/s "
        f"p50={result['latency_p50_ms']:.2f}ms "
        f"p95={result['latency_p95_ms']:.2f}ms "
        f"p99={result['latency_p99_ms']:.2f}ms "
        f"max={result['latency_max_ms']:.2f}ms"
    )


def benchmark_matrix(
    url: str,
    *,
    label: str,
    tool_name: str,
    concurrencies: tuple[int, ...] = (10, 50, 100),
    total_requests: int = 300,
) -> list[dict]:
    """Run benchmark across multiple concurrency levels."""
    results = []

    for concurrency in concurrencies:
        result = benchmark(
            url,
            tool_name=tool_name,
            concurrency=concurrency,
            total_requests=total_requests,
        )
        print_result(f"{label}/{concurrency}", result)
        results.append(result)

    return results


# ============================================================================
# Test Cases
# ============================================================================

def test_compare_local_tool():
    """Benchmark Kurd vs Pure Python on local tool calls."""
    print("\n" + "=" * 80)
    print("BENCHMARK: Local Python Tool")
    print("=" * 80)

    # Start both gateways
    kurd_gateway = None
    python_gateway = None

    try:
        # Start Kurd
        print("\nStarting Kurd gateway...")
        kurd_code = """
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
router.configure_runtime(
    global_concurrency=512,
    upstream_concurrency=128,
    python_concurrency=128,
)

@router.tool()
async def add(a: int, b: int) -> int:
    return a + b

start_http_gateway("127.0.0.1:9200")
"""
        kurd_gateway = subprocess.Popen([sys.executable, "-c", kurd_code])
        wait_for_server(KURD_URL)
        print("[OK] Kurd running on :9200")

        # Start Pure Python
        print("Starting Pure Python gateway...")
        python_gateway = start_pure_python_gateway_process()
        wait_for_server(PURE_PYTHON_URL, timeout=10)
        print("[OK] Pure Python running on :9201")

        # Warm up
        print("\nWarming up both gateways...")
        for i in range(20):
            post_json(KURD_URL, {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "add", "arguments": {"a": 10, "b": 20}},
            })
            post_json(PURE_PYTHON_URL, {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "add", "arguments": {"a": 10, "b": 20}},
            })

        # Benchmark Kurd
        print("\n--- Benchmarking Kurd ---")
        kurd_results = benchmark_matrix(
            KURD_URL,
            label="Kurd/local",
            tool_name="add",
            concurrencies=(10, 50, 100),
            total_requests=300,
        )

        # Benchmark Pure Python
        print("\n--- Benchmarking Pure Python ---")
        python_results = benchmark_matrix(
            PURE_PYTHON_URL,
            label="Python/local",
            tool_name="add",
            concurrencies=(10, 50, 100),
            total_requests=300,
        )

        # Compare
        print("\n" + "=" * 80)
        print("COMPARISON: Kurd vs Pure Python")
        print("=" * 80)

        for kurd_r, python_r in zip(kurd_results, python_results):
            speedup = python_r["throughput_rps"] / kurd_r["throughput_rps"]
            print(f"\nConcurrency {kurd_r['concurrency']}:")
            print(f"  Kurd:        {kurd_r['throughput_rps']:.1f} req/s (p99: {kurd_r['latency_p99_ms']:.2f}ms)")
            print(f"  Pure Python: {python_r['throughput_rps']:.1f} req/s (p99: {python_r['latency_p99_ms']:.2f}ms)")
            print(f"  Speedup:     {speedup:.2f}x (Kurd is {(1 - 1/speedup) * 100:.1f}% faster)")

    finally:
        terminate_process(kurd_gateway)
        terminate_process(python_gateway)


if __name__ == "__main__":
    test_compare_local_tool()
