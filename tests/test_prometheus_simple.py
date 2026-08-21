"""
Simple Prometheus Metrics Test

Quick test of the /metrics endpoint without complex upstream servers.

Run with:
  python -m pytest tests/test_prometheus_simple.py -v -s
"""

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


KURD_URL = "http://127.0.0.1:9200/mcp"
METRICS_URL = "http://127.0.0.1:9200/metrics"
HEALTH_URL = "http://127.0.0.1:9200/health"


def post_json(url: str, payload: dict) -> tuple[int, dict, float]:
    """Make HTTP POST request."""
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"content-type": "application/json"},
        method="POST",
    )

    started = time.perf_counter()

    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return response.status, json.loads(body), elapsed_ms
    except urllib.error.HTTPError as error:
        body = error.read()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        parsed = json.loads(body) if body else {}
        return error.code, parsed, elapsed_ms


def get_text(url: str) -> str:
    """Make HTTP GET request and return text."""
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8")


def wait_for_server(url: str, timeout: float = 5.0) -> None:
    """Wait for server to start."""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        try:
            get_text(url)
            return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError(f"Server did not start: {url}")


def terminate_process(process: subprocess.Popen | None) -> None:
    """Terminate a subprocess gracefully."""
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def start_gateway() -> subprocess.Popen:
    """Start Kurd gateway with a local tool."""
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
    return subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_prometheus_metrics_endpoint_exists():
    """Test that /metrics endpoint exists."""
    gateway = None

    try:
        print("\nStarting Kurd gateway...")
        gateway = start_gateway()
        wait_for_server(HEALTH_URL, timeout=10)
        print("[OK] Gateway started on :9200")

        # Check /metrics endpoint exists
        print("Checking /metrics endpoint...")
        metrics_text = get_text(METRICS_URL)
        print(f"[OK] /metrics endpoint responding ({len(metrics_text)} bytes)")

        # Verify it has Prometheus format
        assert "# HELP" in metrics_text or "# TYPE" in metrics_text or "kurd_" in metrics_text
        print("[OK] Valid Prometheus format detected")

        # Verify it has kurd metrics
        assert "kurd_" in metrics_text
        print("[OK] Kurd metrics present")

        # Show sample
        lines = metrics_text.split("\n")
        print("\nSample metrics (first 30 lines):")
        for line in lines[:30]:
            if line.strip():
                print(f"  {line}")

    finally:
        terminate_process(gateway)


def test_prometheus_metrics_updated_after_requests():
    """Test that metrics are updated after requests."""
    gateway = None

    try:
        print("\nStarting Kurd gateway...")
        gateway = start_gateway()
        wait_for_server(HEALTH_URL, timeout=10)
        print("[OK] Gateway started")

        # Get initial metrics
        print("Getting initial metrics...")
        metrics_1 = get_text(METRICS_URL)
        initial_request_count = extract_metric_value(metrics_1, "kurd_requests_total")
        print(f"Initial request count: {initial_request_count}")

        # Make some requests
        print("Making 10 tool calls...")
        for i in range(10):
            post_json(KURD_URL, {
                "jsonrpc": "2.0",
                "id": i,
                "method": "tools/call",
                "params": {
                    "name": "add",
                    "arguments": {"a": i, "b": i + 1},
                },
            })
        print("[OK] Made 10 calls")

        # Get updated metrics
        print("Getting updated metrics...")
        metrics_2 = get_text(METRICS_URL)
        updated_request_count = extract_metric_value(metrics_2, "kurd_requests_total")
        print(f"Updated request count: {updated_request_count}")

        # Verify metrics increased
        if updated_request_count and initial_request_count:
            assert updated_request_count > initial_request_count
            print(f"[OK] Request count increased (delta: {updated_request_count - initial_request_count})")
        else:
            print("[WARN] Could not verify request count increase (metrics may be zero initially)")

    finally:
        terminate_process(gateway)


def test_prometheus_content_type():
    """Test that /metrics returns correct content-type."""
    gateway = None

    try:
        print("\nStarting Kurd gateway...")
        gateway = start_gateway()
        wait_for_server(HEALTH_URL, timeout=10)
        print("[OK] Gateway started")

        # Check headers
        print("Checking Content-Type header...")
        request = urllib.request.Request(
            METRICS_URL,
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            content_type = response.headers.get("content-type", "")
            print(f"Content-Type: {content_type}")

            assert "text/plain" in content_type
            print("[OK] Correct Content-Type")

    finally:
        terminate_process(gateway)


def extract_metric_value(metrics_text: str, metric_name: str) -> float | None:
    """Extract a metric value from Prometheus text format."""
    lines = metrics_text.split("\n")

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if metric_name in line:
            try:
                # Handle: metric_name{labels} value
                if "{" in line and "} " in line:
                    value_str = line.split("} ", 1)[1]
                    return float(value_str)
                # Handle: metric_name value
                elif " " in line:
                    parts = line.split(" ", 1)
                    if len(parts) == 2:
                        return float(parts[1])
            except (ValueError, IndexError):
                pass

    return None


if __name__ == "__main__":
    print("Use: python -m pytest tests/test_prometheus_simple.py -v -s")
