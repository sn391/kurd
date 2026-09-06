# Changelog

All notable changes to Kurd are documented in this file.

The format is based on Keep a Changelog, and the project follows Semantic Versioning.

## [0.8.0] - 2026-09-06

### Added

- **SSE / Streamable HTTP (`GET /mcp`)** — Clients can now subscribe to `GET /mcp` to receive server-initiated `notifications/tools/list_changed` notifications. All tool and upstream registration events automatically broadcast to connected SSE clients. `initialize` capabilities now advertise `listChanged: true`.
- **`Mcp-Session-Id` session management** — Every `initialize` response carries a unique `Mcp-Session-Id` header (32-char lowercase hex). Client info and capabilities are stored for the session lifetime with automatic high-watermark eviction at 10,000 sessions.
- **JSON-RPC batch requests** — POST bodies containing a JSON array are processed as a batch. Each item is dispatched individually and the responses are returned as a JSON array. Empty arrays return a proper JSON-RPC `-32600` error.
- **Multi-type content in `tools/call`** — Tools can now return MCP content objects directly (`{"type": "image", ...}`, `{"type": "resource", ...}`, etc.) or an array of content objects. Plain string and non-content-object returns are still wrapped in `{"type": "text", "text": ...}` as before.
- **Upstream response size limit** — Upstream responses larger than 32 MiB are rejected before buffering, with a clear error message. Content-Length is checked before reading the body.
- **W3C traceparent propagation to upstream** — Every `tools/call` forwarded to an upstream now carries a `traceparent` header derived from the gateway's own span: same `trace_id`, fresh child `span_id`. Uses a Tokio task-local so the context flows transparently without threading it through every call site.
- **Circuit breaker half-open state** — After the 30-second reset timeout elapses, exactly one probe request is allowed through. All other requests during the probe are rejected with "Circuit half-open". On probe success the circuit fully closes; on probe failure it re-opens immediately with a fresh timeout.
- **`logging/setLevel` reload handle** — The `tracing_subscriber::reload::Handle` is stored globally and used to mutate the live filter on `logging/setLevel` calls. The previous `std::env::set_var` (undefined behaviour in multithreaded Rust) is removed.

### Fixed

- **Critical: `eval()` in `kurd/idempotency.py`** — Replaced `eval(result_json)` with `json.loads(result_json)`, eliminating a remote code execution vulnerability.
- **`std::env::set_var` undefined behaviour** — The `logging/setLevel` handler previously called `set_var` in a multithreaded process. Replaced with the `tracing_subscriber::reload` API.

### Validation

- 133 tests passing across 6 test modules (gateway, admin API, tool filtering, tool discovery, OTEL, new features).

## [0.7.0] - 2026-09-06

### Added

- **Policy engine (P0)** — per-tenant request gating via a Python `TenantManager.validate_request` callback wired directly into the `tools/call` hot path. Forbidden requests return a JSON-RPC `-32004` error. Denials are counted in the Prometheus `kurd_policy_denied_total` metric.
- **Admin HTTP API (P1)** — `GET/POST /admin/servers`, `DELETE /admin/servers/{name}`, `GET /admin/tools`, `POST /admin/tools/reload`, `GET /admin/tools/namespaces`. A dedicated admin bearer token can be set via `Router.set_admin_token()`.
- **Multi-tenant tool filtering (P2)** — `tools/list` returns only the tools a tenant is allowed to call, based on their API key and `allowed_tools` allowlist (supports wildcards). Unknown keys see an empty list.
- **Client-requested tool discovery (P3)** — `params.filter.namespace` scopes results to an upstream; `params.filter.search` performs case-insensitive substring matching on tool name and description. Both filters run after tenant restrictions so tenants cannot enumerate tools outside their allowlist. `_kurd.available` / `_kurd.returned` metadata counts are included in every `tools/list` response.
- **Real OTLP trace export (P4)** — every MCP request emits a W3C-compliant `traceparent` response header. When `Router.configure_otel(endpoint, service_name)` is called, spans are exported fire-and-forget to `{endpoint}/v1/traces` in OTLP JSON format using the existing connection-pooled HTTP client. `setup_otel()` in `kurd.telemetry` now wires into Rust automatically when an endpoint is provided.

### Changed

- `kurd.telemetry.setup_otel()` now activates real Rust-side OTLP export when `endpoint` is set, replacing the previous in-memory stub.

### Validation

- 78 tests passing across 5 test modules (gateway, admin API, tool filtering, tool discovery, OTEL).

## [0.3.0] - 2026-08-20

### Added

- MCP 2026-07-28 protocol support and negotiation.
- `server/discover`, deterministic `tools/list`, and MCP-compatible `tools/call`.
- Modern MCP request metadata and protocol header validation.
- Upstream MCP mounting with namespaced tools.
- Concurrent upstream tool discovery.
- Shared HTTP client connection pooling.
- Upstream retry handling with exponential backoff and jitter.
- Per-upstream circuit breaker.
- Tool discovery cache with TTL, hit/miss counters, and explicit invalidation.
- HTTP gateway lifecycle controls for start, stop, restart, and runtime status.
- Graceful HTTP shutdown.
- Optional HTTP bearer-token authentication.
- MCP request body-size enforcement.
- JSON content-type validation.
- Upstream URL validation.
- Configurable private/loopback upstream policy.
- Configurable upstream timeout.
- Sanitized upstream transport errors.
- Global request concurrency limits and fail-fast backpressure.
- Per-upstream concurrency limits.
- Python callback concurrency limits.
- Tokio blocking-pool isolation for Python callback execution.
- Request ID generation and propagation.
- Optional structured request logging.
- Runtime metrics for requests, latency, concurrency, overloads, and Python callbacks.
- Upstream metrics for success, failure, retries, latency, and circuit-breaker state.
- End-to-end load and burst benchmarks.
- Official MCP Python client interoperability validation.

### Changed

- Python router dispatch now supports both synchronous and asynchronous tool callbacks.
- HTTP forwarding uses a shared reusable Reqwest client.
- Runtime defaults now include production-oriented concurrency limits.
- Package metadata now targets Python 3.10+.
- Project status moved from early development to beta / production hardening.
- Package version synchronized across Rust and Python metadata.

### Fixed

- Prevented Python async callback waiting from blocking Axum/Tokio worker threads.
- Correctly enforced global backpressure during concurrent slow Python tool execution.
- Prevented upstream transport errors from leaking internal OS/network details.
- Prevented duplicate HTTP gateway starts.
- Correctly cleared HTTP lifecycle state after graceful shutdown.

### Validation

- 48/48 regression and integration tests passing.
- 3/3 load benchmark scenarios passing.
- 0% errors in measured local and upstream benchmark runs.
- Sustained burst: 1000 requests at concurrency 100 with 0% errors.
- Official MCP Python client interoperability:
  - protocol: `2026-07-28`
  - tool discovery: `add`
  - tool result: `42`
  - `result_type='complete'`
  - `is_error=False`

## [0.2.0]

### Added

- Initial public PyPI release.
- Python Router API.
- Rust native extension via PyO3 and Maturin.
- Basic JSON-RPC parsing and dispatch.
- Initial MCP gateway foundations.
- Cross-platform CI and automated release workflow.
