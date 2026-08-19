# Kurd v0.3.0

Kurd v0.3.0 is the first production-hardening release of the Rust-powered MCP gateway for Python.

## Highlights

- MCP 2026-07-28 compatibility
- verified interoperability with the official MCP Python client
- local sync and async Python tools
- mounted upstream MCP servers
- retries, circuit breaking, caching, and shared HTTP pooling
- global, upstream, and Python callback backpressure
- graceful HTTP lifecycle management
- bearer authentication and upstream URL security controls
- request IDs, structured logging, and runtime metrics
- end-to-end load and burst benchmarks

## Validation

The v0.3.0 release candidate passed:

- 48 regression/integration tests
- 3 load benchmark scenarios
- 1000-request sustained burst at concurrency 100 with 0% errors
- official MCP Python client negotiation on protocol 2026-07-28
- official MCP tool discovery and tool invocation

## Benchmark Snapshot

Local Python tool:

- ~556-595 requests/second
- 0% errors across concurrency 10/50/100

Upstream tool:

- ~230-412 requests/second
- 0% errors across concurrency 10/50/100

Sustained local burst:

- 1000 requests
- concurrency 100
- ~573 requests/second
- p99 ~218 ms
- 0% errors

These measurements are local development-machine results and are not universal performance guarantees.

## Upgrade

```bash
pip install --upgrade kurd==0.3.0
```
