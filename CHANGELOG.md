# Changelog

All notable changes to Kurd are documented in this file.

The format is based on Keep a Changelog, and the project follows Semantic Versioning.

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
