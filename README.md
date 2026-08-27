# Kurd MCP

[![PyPI](https://img.shields.io/pypi/v/kurd)](https://pypi.org/project/kurd/)
[![Python](https://img.shields.io/pypi/pyversions/kurd)](https://pypi.org/project/kurd/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/sn391/kurd/CI.yml?branch=main)](https://github.com/sn391/kurd/actions)

**Kurd** is a high-performance [Model Context Protocol](https://modelcontextprotocol.io) (MCP) gateway for Python, powered by Rust.

The Rust data plane handles HTTP serving, JSON-RPC dispatch, tool routing, upstream aggregation, caching, retries, circuit breaking, backpressure, rate limiting, and Prometheus metrics. The Python layer provides the developer API — tool registration, runtime configuration, and an optional enterprise feature set.

> Targets MCP protocol revision **2026-07-28**. Fully typed (PEP 561).

---

## Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI](#cli)
- [Registering Tools](#registering-tools)
- [Mounting Upstream Servers](#mounting-upstream-servers)
- [Runtime Configuration](#runtime-configuration)
- [Security](#security)
- [Observability](#observability)
- [MCP Protocol Compliance](#mcp-protocol-compliance)
- [Enterprise Features](#enterprise-features)
- [Performance](#performance)
- [Architecture](#architecture)
- [Development](#development)
- [Project Structure](#project-structure)
- [License](#license)

---

## Installation

```bash
pip install kurd
```

Requires Python 3.10+ and a 64-bit platform. Pre-built wheels are available for Windows, Linux (x86-64, aarch64), and macOS (x86-64, Apple Silicon).

---

## Quick Start

```python
from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()

@router.tool()
async def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b

# Blocks until stop_http_gateway() is called or the process exits.
start_http_gateway("0.0.0.0:9200")
```

The gateway starts three endpoints:

| Path | Method | Purpose |
|------|--------|---------|
| `/mcp` | `POST` | JSON-RPC 2.0 MCP endpoint |
| `/health` | `GET` | Liveness probe — returns `200 OK` |
| `/status` | `GET` | Runtime, cache, upstream, and circuit-breaker snapshot |
| `/metrics` | `GET` | Prometheus metrics |

Call the gateway:

```bash
curl -s http://localhost:9200/mcp \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"add","arguments":{"a":3,"b":4}}}'
```

```json
{"jsonrpc":"2.0","id":1,"result":{"resultType":"complete","content":[{"type":"text","text":"7"}],"isError":false}}
```

---

## CLI

Kurd ships a `kurd` command installed alongside the package.

```
Usage: kurd <COMMAND>

Commands:
  serve   Start the HTTP MCP gateway

Options:
  -h, --help  Show this message and exit
```

### `kurd serve`

```bash
kurd serve [--host HOST] [--port PORT] [--token TOKEN]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8000` | Bind port |
| `--token` | — | Bearer token for authentication (overrides `KURD_AUTH_TOKEN`) |

```bash
# Start on port 8000 with no authentication
kurd serve

# Start on a specific address with a bearer token
kurd serve --host 127.0.0.1 --port 9200 --token my-secret

# Use an environment variable for the token
KURD_AUTH_TOKEN=my-secret kurd serve --port 9200
```

---

## Registering Tools

### Decorator API

```python
from kurd import Router

router = Router()

@router.tool()
async def search(query: str, limit: int = 10) -> list[str]:
    """Search the knowledge base."""
    return [f"result {i}" for i in range(limit)]
```

Type annotations are converted to a JSON Schema `inputSchema` automatically:

| Python type | JSON Schema type |
|-------------|-----------------|
| `int` | `integer` |
| `float` | `number` |
| `bool` | `boolean` |
| `str` | `string` |
| `list[T]` | `array` with item schema |
| `dict` | `object` |
| `Optional[T]` / `T \| None` | schema of `T` |

Parameters with defaults become optional; parameters without defaults are added to `required`.

### Hot-reloading

Replace a tool's implementation at runtime without restarting the gateway:

```python
router.reload_tool("search", new_search_function)
```

### Unregistering

```python
router.unregister_tool("search")
```

### Introspection

```python
router.list_tools()      # -> ["add", "search", ...]
router.list_upstreams()  # -> [("github", "http://..."), ...]
```

---

## Mounting Upstream Servers

Kurd aggregates remote MCP servers alongside local tools.

```python
router.mount("github", "http://github-mcp.internal:9300")
router.mount("jira",   "http://jira-mcp.internal:9300")
```

Upstream tools are prefixed with the upstream name:

```text
github.create_issue
jira.create_ticket
```

Clients discover all tools — local and upstream — through a single `tools/list` call. Kurd fetches remote tool lists concurrently, caches them with a configurable TTL, and follows pagination automatically.

### Unmounting and cache invalidation

```python
router.unmount("github")   # stop routing to this upstream
router.refresh_tools()     # expire the tool list cache immediately
```

### Upstream behaviour

- **Connection pool**: persistent HTTP/1.1 connections via Reqwest
- **Retry**: up to 3 attempts with exponential backoff + jitter
- **Circuit breaker**: opens after 5 consecutive failures; resets after 30 s
- **Timeout**: configurable per `RuntimeConfig.upstream_timeout_ms`
- **Private-network policy**: loopback/private URLs blocked by default unless `set_allow_private_upstreams(True)` is called

---

## Runtime Configuration

All gateway tunables are collected in `RuntimeConfig`:

```python
from kurd import Router, RuntimeConfig

router = Router()
router.configure_runtime(RuntimeConfig(
    # Concurrency
    global_concurrency   = 512,   # max simultaneous in-flight requests
    upstream_concurrency = 64,    # max simultaneous upstream calls
    python_concurrency   = 64,    # max simultaneous Python tool calls
    upstream_timeout_ms  = 30_000,

    # Logging
    request_logging      = False, # structured per-request log lines

    # Rate limiting
    rate_limiting_enabled   = True,
    rate_limit_per_ip_rps   = 1_000,
    rate_limit_global_rps   = 10_000,

    # IP allowlist (None = allow all)
    ip_allowlist         = ["192.168.1.0/24", "10.0.0.1"],

    # Tool cache
    tools_cache_ttl_ms   = 30_000,

    # Enterprise (all off by default)
    enable_dlq                  = False,
    enable_idempotency          = False,
    secrets_backend             = "env",
    enable_webhooks             = False,
    enable_distributed_state    = False,
    distributed_state_backend   = "memory",
    redis_url                   = "redis://localhost:6379/0",
    enable_distributed_tracing  = False,
))
```

`configure_runtime` also accepts keyword arguments directly for ergonomic one-liners:

```python
router.configure_runtime(request_logging=True, rate_limiting_enabled=True)
```

### Runtime status

```python
status = router.runtime_status()
# {
#   "global_active": 3,
#   "global_limit": 512,
#   "python_active": 1,
#   "upstream_metrics": {...},
#   "cache": {"hits": 142, "misses": 3},
#   ...
# }
```

---

## Security

### Bearer token authentication

Set a bearer token before starting the gateway. Requests missing or carrying a wrong token receive `401 Unauthorized`.

```python
from kurd._kurd import set_http_bearer_token, clear_http_bearer_token

set_http_bearer_token("my-production-token")
# clear_http_bearer_token()  # disable authentication
```

Via environment variable (loaded automatically at gateway start):

```bash
KURD_AUTH_TOKEN=my-production-token kurd serve
```

Tokens are compared with a constant-time byte comparison to prevent timing attacks.

### IP allowlist

```python
from kurd import set_ip_allowlist, clear_ip_allowlist

set_ip_allowlist(["10.0.0.1", "10.0.0.2"])
clear_ip_allowlist()  # allow all IPs again
```

Or through `RuntimeConfig.ip_allowlist`. Blocked IPs receive `403 Forbidden`.

### Rate limiting

```python
router.configure_runtime(
    rate_limiting_enabled=True,
    rate_limit_per_ip_rps=1_000,
    rate_limit_global_rps=10_000,
)
```

Rate-limited requests receive `429 Too Many Requests` with a `Retry-After: 1` header and a `retryAfterMs` field in the JSON-RPC error body.

### Additional safeguards

| Safeguard | Details |
|-----------|---------|
| Request size cap | 1 MiB hard limit; `413` on excess |
| Content-type validation | Must be `application/json`; `-32600` otherwise |
| Upstream URL validation | Rejects credentials, fragments, and unsupported schemes |
| Private-network policy | Upstream calls to loopback/RFC1918 blocked by default |
| CORS | `OPTIONS /mcp` returns correct preflight headers; `POST` responses include `Access-Control-Allow-Origin: *` |
| Overload rejection | `503` when global concurrency limit is reached |

For internet-facing deployments, terminate TLS at a reverse proxy (nginx, Caddy, AWS ALB) and apply network-level controls there.

---

## Observability

### Structured logging

Enable per-request log lines (goes to stdout in the format chosen by `KURD_LOG`):

```python
router.configure_runtime(request_logging=True)
```

Control log verbosity via environment variable:

```bash
KURD_LOG=kurd=debug kurd serve   # debug, info, warn, error
RUST_LOG=info kurd serve         # fallback if KURD_LOG is unset
```

Log level can also be changed at runtime via the `logging/setLevel` MCP method.

### Prometheus metrics

```bash
curl http://localhost:9200/metrics
```

| Metric | Type | Description |
|--------|------|-------------|
| `kurd_requests_total{status}` | counter | Total requests by status (`total`, `completed`, `rejected`) |
| `kurd_requests_active` | gauge | In-flight requests right now |
| `kurd_requests_peak_active` | gauge | Highest concurrent request count since startup |
| `kurd_request_latency_ms` | gauge | Rolling average latency (ms) |
| `kurd_request_latency_histogram_ms_bucket{le}` | histogram | Latency distribution (1ms … 5000ms + Inf) |
| `kurd_request_latency_histogram_ms_count` | counter | Total completed requests counted in histogram |
| `kurd_request_latency_histogram_ms_sum` | counter | Total latency (ms) summed across all requests |
| `kurd_python_active_calls` | gauge | Active Python tool invocations |
| `kurd_python_peak_active_calls` | gauge | Peak simultaneous Python tool invocations |
| `kurd_python_rejections_total` | counter | Python tool calls dropped due to concurrency limit |
| `kurd_upstream_requests_total{upstream}` | counter | Requests forwarded per upstream |
| `kurd_upstream_successes_total{upstream}` | counter | Successful upstream calls |
| `kurd_upstream_failures_total{upstream}` | counter | Failed upstream calls |
| `kurd_upstream_retries_total{upstream}` | counter | Retry attempts per upstream |
| `kurd_upstream_latency_ms{upstream}` | gauge | Average upstream round-trip latency |
| `kurd_upstream_circuit_breaker_state{upstream}` | gauge | `0` = closed, `1` = open |
| `kurd_upstream_active_calls{upstream}` | gauge | Current in-flight calls per upstream |
| `kurd_upstream_peak_active_calls{upstream}` | gauge | Peak in-flight calls per upstream |
| `kurd_upstream_rejections_total` | counter | Upstream calls dropped due to concurrency limit |
| `kurd_cache_hits_total` | counter | Tool-list cache hits |
| `kurd_cache_misses_total` | counter | Tool-list cache misses |
| `kurd_cache_invalidations_total` | counter | Cache invalidations (manual or TTL expiry) |
| `kurd_concurrency_limit{type}` | gauge | Configured limits: `global`, `upstream`, `python` |

#### Prometheus scrape config

```yaml
# prometheus.yml
scrape_configs:
  - job_name: kurd
    static_configs:
      - targets: ["localhost:9200"]
    metrics_path: /metrics
    scrape_interval: 15s
```

#### Datadog

```yaml
# datadog.yaml
instances:
  - openmetrics_endpoint: http://localhost:9200/metrics
    namespace: kurd
    metrics: ["kurd_.*"]
```

### OpenTelemetry

```python
from kurd.telemetry import setup_otel, OTELConfig

setup_otel(OTELConfig(
    service_name    = "my-gateway",
    otlp_endpoint   = "http://otel-collector:4317",
    sample_rate     = 1.0,
))
```

`OTELConfig.service_version` defaults to the installed `kurd` package version automatically.

### Health checks

```python
from kurd.health_checks import HealthCheckManager

hc = HealthCheckManager()

# Register a custom check
async def check_db():
    ...

hc.register_check("database", check_db, critical=True)

# Kubernetes probes
readiness = await hc.check_readiness()  # all critical checks pass
liveness  = await hc.check_liveness()   # process is running and active
```

---

## MCP Protocol Compliance

Kurd implements the MCP **2026-07-28** protocol revision.

### Supported methods

| Method | Behaviour |
|--------|-----------|
| `initialize` | Returns `protocolVersion`, `capabilities`, and `serverInfo` |
| `ping` | Returns `{}` |
| `server/discover` | Returns capabilities, supported versions, and server identity |
| `tools/list` | Aggregates local + upstream tools; supports cursor-based pagination |
| `tools/call` | Routes to local Python tool or upstream server |
| `resources/list` | Returns empty list with `ttlMs` and `cacheScope` |
| `resources/read` | Returns `{"contents": []}` |
| `prompts/list` | Returns empty list with `ttlMs` and `cacheScope` |
| `prompts/get` | Returns `-32602` (gateway holds no prompts) |
| `completion/complete` | Returns `{"values": [], "hasMore": false}` |
| `logging/setLevel` | Applies log level to the tracing filter at runtime |
| `notifications/*` | Accepted silently — returns `202 Accepted` with empty body |

### Modern HTTP headers

When a client sends `Mcp-Protocol-Version: 2026-07-28`, Kurd additionally validates:

- `Mcp-Method` header matches the JSON-RPC `method` field
- `Mcp-Name` header matches `params.name` for `tools/call`

Mismatched headers return `-32020`. Unsupported protocol versions return `-32019`.

---

## Enterprise Features

Enable features through `RuntimeConfig` or by importing the relevant manager class directly.

### Multi-tenancy

```python
from kurd.multitenancy import TenantManager

manager = TenantManager()
api_key = manager.add_tenant(
    tenant_id="acme",
    name="Acme Corp",
    quota_rps=100,
    allowed_tools=["add", "search"],
)
```

Each tenant receives a unique API key. Quotas and tool ACLs are enforced independently.

### Billing

```python
from kurd.billing import BillingManager

billing = BillingManager()
billing.set_pricing({"add": {"per_call": 0.001, "per_latency_ms": 0.0001}})
billing.track_call(tenant_id="acme", tool_name="add", latency_ms=12.5, success=True)

report = billing.get_usage_report("acme", period="2026-08")
```

Supported models: per-request, per-latency, tiered, hybrid.

### Request idempotency

```python
router.configure_runtime(enable_idempotency=True)
mgr = router.get_idempotency()

is_dup, cached = mgr.check_idempotent_key("req-abc-123", tenant_id="acme")
if is_dup:
    return cached

result = run_tool()
mgr.store_result("req-abc-123", "acme", result)
```

Backed by SQLite with a 24-hour TTL.

### Dead-letter queue

```python
router.configure_runtime(enable_dlq=True, dlq_storage_path="/data/kurd/dlq")
dlq = router.get_dlq()

dlq.add_message(request_id="req-123", tenant_id="acme",
                tool_name="add", arguments={"a":1,"b":2}, error="timeout")

dlq.register_replay_handler("add", handler)
success, error = dlq.replay_message("dlq_abc123")

stats = dlq.get_statistics(tenant_id="acme")
dlq.cleanup_archived(days=30)
```

Replay uses exponential backoff up to 1 hour.

### Secrets management

```python
from kurd.secrets_management import SecretsManager

# Kubernetes in-cluster | HashiCorp Vault | AWS Secrets Manager | env (default)
manager = SecretsManager(backend="vault",
                          vault_addr="https://vault.example.com",
                          vault_token="s.xxxxx")

secret = manager.get_secret("db_password")
```

Third-party dependencies (`kubernetes`, `hvac`, `boto3`) are imported lazily — only when the matching backend is activated.

### Webhooks

```python
router.configure_runtime(enable_webhooks=True)
hooks = router.get_webhooks()

hooks.register_webhook(
    url="https://example.com/hooks",
    events=["error", "rate_limit_exceeded"],
    tenant_id="acme",
)
```

Deliveries are HMAC-SHA256 signed and logged for audit via `get_deliveries()`.

### Distributed state

```python
router.configure_runtime(
    enable_distributed_state=True,
    distributed_state_backend="redis",
    redis_url="redis://localhost:6379/0",
)
state = router.get_distributed_state()
state.set("gateway:version", 42)
state.increment("counters:acme:calls")
```

Use `backend="memory"` for single-instance deployments.

### Distributed tracing

```python
from kurd.distributed_tracing import extract_context

trace = extract_context(incoming_headers)
span  = trace.create_span("tool_execution", {"tool": "add"})
span.set_attribute("result", 42)
span.end()
```

Follows W3C Trace Context. Tracing state is available in `router.runtime_status()` when enabled.

---

## Performance

Benchmarks from a Windows development machine (Python 3.12, release build):

| Scenario | Concurrency | Throughput | p50 | p95 | p99 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| Local Python tool | 10 | 594.5 req/s | 14.9 ms | 23.9 ms | 28.7 ms | 0% |
| Local Python tool | 50 | 587.9 req/s | 33.3 ms | 87.8 ms | 119.1 ms | 0% |
| Local Python tool | 100 | 556.0 req/s | 18.3 ms | 29.5 ms | 32.4 ms | 0% |
| Upstream tool | 10 | 412.2 req/s | 21.8 ms | 36.4 ms | 42.7 ms | 0% |
| Upstream tool | 50 | 229.8 req/s | 20.6 ms | 534.6 ms | 549.3 ms | 0% |
| Sustained burst | 100 | 573.3 req/s | 73.5 ms | 179.1 ms | 218.5 ms | 0% |

Results depend on hardware, OS, Python version, and network conditions.

```bash
python -m pytest tests/test_load.py -q -s
```

---

## Architecture

```
Python application
       │
       ▼
  kurd.Router                      ← Python API layer
       │
       ├── Enterprise modules (optional, lazy)
       │   multitenancy · billing · idempotency · DLQ
       │   secrets · webhooks · distributed state · tracing
       │
       ▼
   PyO3 boundary
       │
       ▼
  Rust MCP gateway (Axum + Tokio)
       │
       ├── HTTP handler  ─────────────────────────────────┐
       │   content-type · auth · IP allowlist              │
       │   rate limiting · concurrency backpressure        │
       │   CORS · request ID · tracing                     │
       │                                                   │
       ├── MCP dispatcher                                  │
       │   initialize · ping · server/discover             │
       │   tools/list (paginated) · tools/call             │
       │   resources · prompts · completion · logging      │
       │   notifications (202)                             │
       │                                                   │
       ├── Local Python tools ◄── PyO3 callback            │
       │   (Rayon-parallel batch parsing)                  │
       │                                                   │
       └── Upstream MCP servers                           │
           retry · circuit breaker · cache · metrics      ◄┘
```

The Rust layer holds all mutable gateway state in lock-free atomics and `RwLock`-guarded maps. Python code never touches the hot path after registration.

---

## Development

### Prerequisites

- Rust stable toolchain (`rustup update stable`)
- Python 3.10+
- `maturin` and `pytest`

```bash
pip install maturin pytest
```

### Build

```bash
# Development build (fast iteration)
maturin develop

# Optimised build (benchmarks, pre-release testing)
maturin develop --release

# Release wheel
maturin build --release
```

### Test

```bash
python -m pytest -q
```

The test suite covers:

- JSON-RPC parsing and fast batch parsing (Rayon)
- Local sync and async tools
- `initialize` handshake and lifecycle methods
- `tools/list` pagination
- `completion/complete`, `notifications/202`, CORS preflight
- Upstream discovery, routing, and concurrency
- Circuit breaker, retry, and timeout behaviour
- Tool-list cache hits, misses, and invalidation
- Bearer authentication (accepted and rejected)
- IP allowlist enforcement
- Rate-limit rejection and `Retry-After` header
- Request-size and content-type guards
- Prometheus metrics output
- Load and burst behaviour

### Linting

```bash
cargo check
cargo clippy -- -D warnings
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `KURD_AUTH_TOKEN` | Bearer token loaded automatically at gateway start |
| `KURD_LOG` | Tracing filter (e.g. `kurd=debug`). Takes precedence over `RUST_LOG` |
| `RUST_LOG` | Standard Rust log filter fallback |

---

## Project Structure

```
kurd-mcp/
├── kurd/
│   ├── __init__.py            # Public API + __version__
│   ├── py.typed               # PEP 561 marker
│   ├── cli.py                 # `kurd serve` entry point
│   ├── router.py              # Router class + RuntimeConfig
│   ├── telemetry.py           # OpenTelemetry integration
│   ├── health_checks.py       # Readiness and liveness probes
│   ├── authorization.py       # RBAC helpers
│   ├── multitenancy.py
│   ├── billing.py
│   ├── idempotency.py
│   ├── dead_letter_queue.py
│   ├── secrets_management.py
│   ├── webhooks.py
│   ├── distributed_state.py
│   ├── distributed_tracing.py
│   └── ...
├── src/
│   └── lib.rs                 # Rust data plane (~2600 lines)
├── tests/
│   ├── test_core.py
│   ├── test_http_gateway.py   # Integration tests (module-scoped gateway)
│   ├── test_upstream.py
│   ├── test_load.py
│   ├── test_prometheus_metrics.py
│   └── upstream_server.py     # In-process upstream fixture
├── Cargo.toml
├── pyproject.toml
├── LICENSE
└── README.md
```

---

## Contributing

Issues and pull requests are welcome via the [GitHub repository](https://github.com/sn391/kurd).

Before submitting:

```bash
cargo check
cargo clippy -- -D warnings
maturin develop --release
python -m pytest -q
```

Please open an issue before starting large changes.

---

## License

[MIT](LICENSE) — Copyright © 2024 Semko Kermashani

---

*The name **Kurd** honors Kurdish identity and heritage. Bezhi Kurd u Kurdistan.*
