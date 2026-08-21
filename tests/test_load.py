import concurrent.futures
import json
import math
import os
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


KURD_URL = "http://127.0.0.1:9200/mcp"
UPSTREAM_SERVER = Path(__file__).with_name("upstream_server.py")


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


def wait_for_server(url: str, timeout: float = 5.0, process: subprocess.Popen | None = None) -> None:
    deadline = time.perf_counter() + timeout

    while time.perf_counter() < deadline:
        if process and process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(f"Server process exited prematurely.\nStdout: {stdout}\nStderr: {stderr}")

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

    error_msg = f"Server did not start: {url}"
    if process:
        try:
            if process.poll() is None:
                stdout, stderr = process.communicate(timeout=1)
                error_msg += f"\nServer still running but not responding.\nStdout: {stdout}\nStderr: {stderr}"
            else:
                stdout, stderr = process.communicate()
                error_msg += f"\nServer process exited.\nStdout: {stdout}\nStderr: {stderr}"
        except:
            pass
    raise RuntimeError(error_msg)


def terminate_process(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return

    process.terminate()

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def start_upstream(
    port: int,
    tool_name: str = "add",
    *,
    call_delay: float = 0.0,
) -> subprocess.Popen:
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
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def start_local_gateway() -> subprocess.Popen:
    code = """
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
router.configure_runtime(
    global_concurrency=512,
    upstream_concurrency=128,
    python_concurrency=128,
)

@router.tool()
def add(a: int, b: int) -> int:
    return a + b

start_http_gateway("127.0.0.1:9200")
"""
    return subprocess.Popen([sys.executable, "-c", code])


def start_upstream_gateway() -> subprocess.Popen:
    code = """
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()
router.configure_runtime(
    global_concurrency=512,
    upstream_concurrency=128,
    python_concurrency=128,
)
router.mount("remote", "http://localhost:9100")

start_http_gateway("127.0.0.1:9200")
"""
    return subprocess.Popen([sys.executable, "-c", code])


def benchmark(
    *,
    tool_name: str,
    concurrency: int,
    total_requests: int,
) -> dict:
    latencies = []
    successes = 0
    failures = 0
    overloads = 0

    def one_call(index: int):
        return post_json(
            KURD_URL,
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": {
                        "a": index,
                        "b": 1,
                    },
                },
            },
        )

    started = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrency
    ) as executor:
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
    *,
    label: str,
    tool_name: str,
    concurrencies: tuple[int, ...] = (10, 50, 100),
    total_requests: int = 300,
) -> list[dict]:
    results = []

    for concurrency in concurrencies:
        result = benchmark(
            tool_name=tool_name,
            concurrency=concurrency,
            total_requests=total_requests,
        )
        print_result(f"{label}/{concurrency}", result)
        results.append(result)

    return results


def test_load_local_python_tool():
    gateway = None

    try:
        gateway = start_local_gateway()
        wait_for_server(KURD_URL)

        # Warm-up.
        for index in range(20):
            status, response, _ = post_json(
                KURD_URL,
                {
                    "jsonrpc": "2.0",
                    "id": 10_000 + index,
                    "method": "tools/call",
                    "params": {
                        "name": "add",
                        "arguments": {"a": 20, "b": 22},
                    },
                },
            )
            assert status == 200
            assert response["result"]["isError"] is False

        results = benchmark_matrix(
            label="local",
            tool_name="add",
        )

        for result in results:
            assert result["successes"] == result["requests"]
            assert result["error_rate"] == 0.0
            assert result["throughput_rps"] > 0
            assert result["latency_p99_ms"] > 0

    finally:
        terminate_process(gateway)


def test_load_upstream_tool():
    upstream = None
    gateway = None

    try:
        upstream = start_upstream(
            port=9100,
            tool_name="add",
            call_delay=0.005,
        )
        wait_for_server("http://127.0.0.1:9100", process=upstream)

        gateway = start_upstream_gateway()
        wait_for_server(KURD_URL)

        # Warm-up.
        for index in range(10):
            status, response, _ = post_json(
                KURD_URL,
                {
                    "jsonrpc": "2.0",
                    "id": 20_000 + index,
                    "method": "tools/call",
                    "params": {
                        "name": "remote.add",
                        "arguments": {"a": 20, "b": 22},
                    },
                },
            )
            assert status == 200
            assert response["result"]["isError"] is False

        results = benchmark_matrix(
            label="upstream",
            tool_name="remote.add",
        )

        for result in results:
            assert result["successes"] == result["requests"]
            assert result["error_rate"] == 0.0
            assert result["throughput_rps"] > 0
            assert result["latency_p99_ms"] > 0

    finally:
        terminate_process(gateway)
        terminate_process(upstream)


def test_load_sustained_burst_stays_healthy():
    gateway = None

    try:
        gateway = start_local_gateway()
        wait_for_server(KURD_URL)

        result = benchmark(
            tool_name="add",
            concurrency=100,
            total_requests=1000,
        )
        print_result("local/sustained-burst", result)

        assert result["successes"] == 1000
        assert result["failures"] == 0
        assert result["error_rate"] == 0.0

        with urllib.request.urlopen(
            "http://127.0.0.1:9200/status",
            timeout=5,
        ) as response:
            status = json.loads(response.read())

        runtime = status["runtime"]
        assert runtime["activeRequests"] == 0
        assert runtime["completedRequests"] >= 1001
        assert runtime["peakActiveRequests"] >= 1

    finally:
        terminate_process(gateway)


if __name__ == "__main__":
    # Optional direct runner:
    #   python tests/test_load.py
    raise SystemExit(
        "Run with: python -m pytest tests/test_load.py -q -s"
    )
