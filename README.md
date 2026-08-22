# Kurd

A high-performance Model Context Protocol (MCP) gateway for Python, powered by Rust.

Kurd combines a Python-first developer API with a Rust data plane for MCP routing, upstream aggregation, concurrency control, security, caching, and observability. The optional enterprise layer adds multi-tenancy, billing, idempotency, a dead-letter queue, secrets management, webhooks, distributed state, and distributed tracing.

## Status

Kurd is in **beta** and is being hardened for production use.

Current release line: **0.4.x**

The gateway targets the MCP **2026-07-28** protocol revision while preserving compatibility paths used by existing Kurd applications.

## Highlights

### Core gateway

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
- Prometheus metrics export
- Cross-platform CI and automated PyPI release workflow

### Enterprise layer

- Multi-tenancy with per-tenant API keys, quotas, and tool ACLs
- Billing and usage tracking with configurable pricing models
- Request idempotency (SQLite-backed, 24-hour result TTL)
- Dead-letter queue with exponential-backoff replay
- Secrets management (Kubernetes, HashiCorp Vault, AWS Secrets Manager, env)
- Webhook notifications for gateway events
- Distributed state (Redis or in-memory)
- W3C-compatible distributed tracing context propagation

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

Kurd provides a production security baseline:

- maximum MCP request body size (1 MiB)
- JSON content-type validation
- optional bearer-token authentication with constant-time comparison
- upstream URL validation (scheme, credentials, fragment)
- configurable private/loopback upstream policy
- configurable upstream request timeout
- sanitized upstream transport errors
- overload rejection through explicit backpressure
- per-IP and global rate limiting

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

## Enterprise Features

### Multi-Tenancy

Isolate tools, quotas, and API keys per tenant:

```python
from kurd.multitenancy import TenantManager

manager = TenantManager()
api_key = manager.add_tenant(
    tenant_id="acme-corp",
    name="Acme Corp",
    quota_rps=100,
    allowed_tools=["add", "multiply"],
)
```

Each tenant gets a unique API key. The manager enforces per-tenant RPS quotas and tool access control lists independently.

### Billing & Usage Tracking

Track tool usage per tenant with configurable pricing:

```python
from kurd.billing import BillingManager

billing = BillingManager()
billing.set_pricing({
    "add": {"per_call": 0.001, "per_latency_ms": 0.0001},
})

billing.track_call(
    tenant_id="acme-corp",
    tool_name="add",
    latency_ms=25.5,
    success=True,
)

report = billing.get_usage_report("acme-corp", period="2026-08")
```

Supported billing models: per-request, per-latency, tiered, and hybrid.

### Request Idempotency

Prevent duplicate tool executions using idempotency keys:

```python
router.configure_runtime(enable_idempotency=True)
idempotency = router.get_idempotency()

is_duplicate, cached = idempotency.check_idempotent_key(
    idempotency_key="req-abc-123",
    tenant_id="acme-corp",
)
if is_duplicate:
    return cached

result = process_request()
idempotency.store_result("req-abc-123", "acme-corp", result)
```

Results are stored in SQLite with a 24-hour TTL by default.

### Dead-Letter Queue

Capture failed requests for later replay:

```python
router.configure_runtime(enable_dlq=True, dlq_storage_path="/data/kurd/dlq")
dlq = router.get_dlq()

dlq.add_message(
    request_id="req-123",
    tenant_id="acme-corp",
    tool_name="add",
    arguments={"a": 1, "b": 2},
    error="Timeout after 30s",
)

dlq.register_replay_handler("add", add_handler)
success, error = dlq.replay_message("dlq_abc123")

pending = dlq.get_pending_replays()
stats = dlq.get_statistics(tenant_id="acme-corp")
```

Replay uses exponential backoff (up to 1 hour) and a configurable maximum retry count. Archived messages are cleaned up via `cleanup_archived(days=30)`.

### Secrets Management

Retrieve secrets from Kubernetes, HashiCorp Vault, AWS Secrets Manager, or environment variables:

```python
from kurd.secrets_management import SecretsManager

# Kubernetes (in-cluster)
manager = SecretsManager(backend="kubernetes")

# HashiCorp Vault
manager = SecretsManager(
    backend="vault",
    vault_addr="https://vault.example.com",
    vault_token="s.xxxxx",
)

# AWS Secrets Manager
manager = SecretsManager(backend="aws", aws_region="us-east-1")

# Environment variables (default)
manager = SecretsManager(backend="env")

secret = manager.get_secret("db_password")
```

Secrets are cached locally until `clear_cache()` is called. Required third-party packages (`kubernetes`, `hvac`, `boto3`) are only imported when the corresponding backend is activated.

### Webhooks

Receive event-driven notifications for gateway events:

```python
router.configure_runtime(enable_webhooks=True)
webhooks = router.get_webhooks()

webhooks.register_webhook(
    url="https://example.com/hooks",
    events=["error", "dlq_replay_failed", "rate_limit_exceeded"],
    tenant_id="acme-corp",
)

webhooks.trigger_event(
    event_type="error",
    tenant_id="acme-corp",
    data={"tool": "add", "error": "timeout"},
)
```

Supported events: `error`, `dlq_message_added`, `dlq_replay_success`, `dlq_replay_failed`, `rate_limit_exceeded`, `health_check_failed`, `request_timeout`, `authorization_failed`, `idempotent_duplicate`.

Deliveries are signed with HMAC-SHA256 and stored for audit via `get_deliveries()`.

### Distributed State

Share state across multiple Kurd instances using Redis or in-memory storage:

```python
router.configure_runtime(
    enable_distributed_state=True,
    distributed_state_backend="redis",
    redis_url="redis://localhost:6379/0",
)
state = router.get_distributed_state()

state.set("gateway:config:version", 42)
version = state.get("gateway:config:version")

state.increment("counters:acme-corp:calls")
state.append_to_list("events:acme-corp", {"type": "tool_call"})
```

Use the `memory` backend for local development or single-instance deployments.

### Distributed Tracing

Propagate W3C Trace Context across services:

```python
from kurd.distributed_tracing import extract_context, inject_context

trace = extract_context(incoming_headers)
span = trace.create_span("tool_execution", {"tool": "add"})
span.set_attribute("result", 42)
span.end()

upstream_headers = inject_context(trace)
```

Tracing context is accessible from `router.get_tracing_context()` and is included in `runtime_status()` output when enabled.

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
| Upstream tool | 100 | 293.6 req/s | 30.12 ms | 531.40 ms | 535.40 ms | 0% |
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
   (Python API layer)
       |
       +-- Enterprise modules (optional)
       |   multitenancy, billing, idempotency,
       |   DLQ, secrets, webhooks,
       |   distributed state, tracing
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

Rust owns the HTTP server, MCP validation, routing, caching, retries, circuit breaking, backpressure, rate limiting, and operational metrics. Python provides the developer-facing registration, configuration, and enterprise-feature APIs.

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
- Prometheus metrics export
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
│   ├── router.py
│   ├── multitenancy.py
│   ├── billing.py
│   ├── idempotency.py
│   ├── dead_letter_queue.py
│   ├── secrets_management.py
│   ├── webhooks.py
│   ├── distributed_state.py
│   ├── distributed_tracing.py
│   ├── api_key_management.py
│   ├── audit_logging.py
│   ├── authorization.py
│   ├── error_recovery.py
│   ├── graceful_shutdown.py
│   ├── health_checks.py
│   ├── persistence.py
│   ├── request_response_logging.py
│   ├── request_validation.py
│   ├── resource_limits.py
│   ├── telemetry.py
│   └── tls_management.py
├── src/
│   └── lib.rs
├── tests/
│   ├── test_core.py
│   ├── test_upstream.py
│   ├── test_load.py
│   ├── test_prometheus_metrics.py
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
