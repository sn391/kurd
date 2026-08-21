# Kurd

A high-performance Model Context Protocol (MCP) gateway for Python, powered by Rust.

Kurd combines a Python-first developer API with a Rust data plane for MCP routing, upstream aggregation, concurrency control, security, caching, and observability.

## Status

Kurd is in **beta** and is being hardened for production use.

Current release line: **0.3.x**

The gateway targets the MCP **2026-07-28** protocol revision while preserving compatibility paths used by existing Kurd applications.

## Highlights

- Python-first `Router` API
- Rust core using Tokio, Axum, Serde, and Reqwest
- MCP `server/discover`, `tools/list`, and `tools/call`
- Local Python tools and mounted upstream MCP servers
- Sync and async Python callbacks
- Concurrent upstream discovery
- Shared HTTP connection pool
- Retry with exponential backoff and jitter
- Circuit breaker
- Tool-list caching with TTL and cache scope
- Graceful HTTP lifecycle: start, stop, status, restart
- Optional bearer authentication
- Request-size and content-type validation
- Upstream URL validation and private-network policy
- Configurable upstream timeout
- Global, per-upstream, and Python callback backpressure
- Request IDs and structured request logging
- Runtime, cache, and upstream metrics
- Cross-platform CI and automated PyPI release workflow

## Installation

```bash
pip install kurd
```

Python 3.10 or newer is required.

## Quick Start

```python
from kurd import Router

router = Router()

@router.tool()
async def add(a: int, b: int) -> int:
    return a + b
```

Start the HTTP gateway:

```python
from kurd._kurd import start_http_gateway

start_http_gateway("127.0.0.1:9200")
```

The MCP endpoint is:

```text
http://127.0.0.1:9200/mcp
```

Health and operational status are exposed at:

```text
GET /health
GET /status
GET /metrics
```

The `/metrics` endpoint exports Prometheus-format metrics for integration with monitoring systems (Datadog, Prometheus, New Relic, etc.).

## Mount an Upstream MCP Server

```python
from kurd import Router

router = Router()
router.mount("github", "http://127.0.0.1:9300")
```

An upstream tool named `create_issue` is exposed through Kurd as:

```text
github.create_issue
```

Unmount or refresh the aggregated tool cache:

```python
router.unmount("github")
router.refresh_tools()
```

## Runtime Hardening

Kurd provides explicit concurrency controls:

```python
router.configure_runtime(
    global_concurrency=512,
    upstream_concurrency=64,
    python_concurrency=64,
    request_logging=False,
)
```

Inspect runtime state:

```python
print(router.runtime_status())
```

The HTTP `/status` endpoint also reports runtime, cache, security, upstream latency, retry, and circuit-breaker metrics.

## Security

Kurd currently provides a production security baseline:

- maximum MCP request body size
- JSON content-type validation
- optional bearer-token authentication
- upstream URL validation
- configurable private/loopback upstream policy
- configurable upstream request timeout
- sanitized upstream transport errors
- overload rejection through explicit backpressure

For deployments exposed beyond localhost, use TLS at the reverse proxy or ingress layer and apply your normal network-level authentication and authorization controls.

## Observability & Monitoring

### Prometheus Metrics Export

Kurd exports metrics in Prometheus format at the `/metrics` endpoint:

```bash
curl http://127.0.0.1:9200/metrics
```

**Available metrics:**

- `kurd_requests_total` - Total HTTP requests (total, completed, rejected)
- `kurd_requests_active` - Currently active requests
- `kurd_requests_peak_active` - Peak concurrent requests
- `kurd_request_latency_ms` - Average request latency
- `kurd_python_active_calls` - Active Python tool calls
- `kurd_python_rejections_total` - Python tool call rejections
- `kurd_upstream_requests_total` - Requests to upstream servers (per upstream)
- `kurd_upstream_successes_total` - Successful upstream calls
- `kurd_upstream_failures_total` - Failed upstream calls
- `kurd_upstream_retries_total` - Upstream call retries
- `kurd_upstream_latency_ms` - Average upstream latency
- `kurd_upstream_circuit_breaker_state` - Circuit breaker state (0=closed, 1=open)
- `kurd_cache_hits_total` - Tool discovery cache hits
- `kurd_cache_misses_total` - Tool discovery cache misses
- `kurd_cache_invalidations_total` - Cache invalidations
- `kurd_concurrency_limit` - Configured concurrency limits

**Integration example (Prometheus):**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kurd'
    static_configs:
      - targets: ['127.0.0.1:9200']
    metrics_path: '/metrics'
```

**Integration example (Datadog):**

```yaml
# datadog.yaml
openmetrics_endpoint: http://127.0.0.1:9200/metrics
```

## MCP 2026-07-28

Kurd implements the stateless 2026 MCP model used for routable gateway traffic:

- per-request protocol metadata
- `MCP-Protocol-Version`
- `Mcp-Method`
- `Mcp-Name` for tool calls
- `server/discover`
- deterministic `tools/list`
- `resultType`
- `ttlMs`
- `cacheScope`
- server identity metadata

Kurd rejects mismatched modern MCP headers and unsupported protocol versions.

## Performance

The repository includes end-to-end HTTP load tests in `tests/test_load.py`.

Example measurements from a Windows development machine:

| Scenario | Concurrency | Throughput | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| Local Python tool | 10 | 594.5 req/s | 14.94 ms | 23.88 ms | 28.66 ms | 0% |
| Local Python tool | 50 | 587.9 req/s | 33.29 ms | 87.83 ms | 119.09 ms | 0% |
| Local Python tool | 100 | 556.0 req/s | 18.27 ms | 29.52 ms | 32.43 ms | 0% |
| Upstream tool | 10 | 412.2 req/s | 21.77 ms | 36.35 ms | 42.74 ms | 0% |
| Upstream tool | 50 | 229.8 req/s | 20.61 ms | 534.61 ms | 549.25 ms | 0% |
| Upstream tool | 100 | 293.6 req/s | 30.12 ms | 531.40 ms | 535.34 ms | 0% |
| Local sustained burst | 100 | 573.3 req/s | 73.51 ms | 179.13 ms | 218.49 ms | 0% |

These are local measurements, not universal performance guarantees. Hardware, operating system, Python version, payload shape, upstream implementation, and network conditions affect results.

Run the benchmark suite with:

```bash
python -m pytest tests/test_load.py -q -s
```

## Development

Create and activate a virtual environment, then install the development tools:

```bash
python -m pip install --upgrade pip
python -m pip install maturin pytest
```

Build the native extension:

```bash
maturin develop --release
```

Run the full test suite:

```bash
python -m pytest -q
```

Build release artifacts:

```bash
maturin build --release
```

## Architecture

```text
Python application
       |
       v
   Kurd Router
       |
       v
   PyO3 boundary
       |
       v
 Rust MCP gateway
   |          |
   |          +--> Local Python tools
   |
   +-------------> Upstream MCP servers
```

Rust owns the HTTP server, MCP validation, routing, caching, retries, circuit breaking, backpressure, and operational metrics. Python provides the developer-facing registration and configuration API.

## Testing

The current suite covers:

- JSON-RPC parsing and dispatch
- local sync and async tools
- upstream discovery and calls
- concurrent upstream discovery
- cache behavior and invalidation
- mount and unmount
- MCP 2026 request headers and protocol-version checks
- HTTP lifecycle and graceful shutdown
- request-size and content-type security
- bearer authentication
- upstream URL policy
- timeout configuration
- error sanitization
- global and Python callback backpressure
- request ID propagation
- runtime observability
- load and burst behavior

## Compatibility

CI targets Windows, Linux, and macOS. Release wheels are built through Maturin.

The project is primarily developed with Python 3.12 and stable Rust; package metadata supports Python 3.10+.

## Release Policy

Kurd uses semantic versioning while the public API stabilizes.

- patch releases: bug fixes and packaging corrections
- minor releases: new gateway or MCP capabilities
- `1.0.0`: stable public API commitment

## Project Structure

```text
kurd/
├── kurd/
│   ├── __init__.py
│   └── router.py
├── src/
│   └── lib.rs
├── tests/
│   ├── test_upstream.py
│   ├── test_load.py
│   └── upstream_server.py
├── Cargo.toml
├── pyproject.toml
├── README.md
└── LICENSE
```

## Contributing

Issues and technical discussions are welcome through the GitHub issue tracker.

Before submitting a change:

```bash
cargo check
maturin develop --release
python -m pytest -q
```

## License

MIT.

## Name

The name **Kurd** honors Kurdish identity and heritage.

Bezhi Kurd u Kurdistan.
