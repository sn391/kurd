# Kurd

A high-performance Model Context Protocol (MCP) gateway for Python, powered by Rust.

## Status

Kurd is currently in early development.

The public API and internal architecture may change before the first stable release.

## Highlights

- Python-first developer experience
- Rust-powered native core
- Fast JSON-RPC preprocessing
- Async routing support
- Native extension built with PyO3
- Packaging and distribution with Maturin
- Designed for high-throughput MCP workloads

## Installation

```bash
pip install kurd
```

> Kurd is currently in early development. Platform-specific wheels may not yet be available for every Python version and operating system.

## Quick Start

```python
from kurd import Router

router = Router()


@router.tool(name="ping")
async def ping(value: int):
    return value + 1
```

## JSON-RPC Dispatch

Kurd can route JSON-RPC requests to registered asynchronous Python tools.

```python
import asyncio

from kurd import Router


router = Router()


@router.tool(name="add")
async def add(a: int, b: int):
    return a + b


async def main():
    response = await router.dispatch(
        '{"jsonrpc":"2.0","id":1,"method":"add","params":{"a":2,"b":3}}'
    )

    print(response)


asyncio.run(main())
```

Example response:

```json
{
  "jsonrpc": "2.0",
  "result": 5,
  "id": "1"
}
```

## Architecture

Kurd uses a hybrid Python and Rust architecture.

```text
Python API
    |
    v
Kurd Router
    |
    v
PyO3
    |
    v
Rust Core
    |
    v
JSON-RPC Processing
```

Python provides the developer-facing API, while performance-sensitive parsing and preprocessing are handled by the Rust core.

## Performance

Early local microbenchmarks show that Kurd's Rust JSON-RPC preprocessing path can outperform an equivalent pure-Python implementation.

Current measurements are experimental and should not yet be interpreted as production performance guarantees.

Benchmarking is being performed with tools such as `pyperf` to measure:

- throughput
- average latency
- p50 latency
- p95 latency
- p99 latency
- Python vs. Rust preprocessing performance

Reproducible benchmark results will be published as the project matures.

## Development

Kurd requires Python and Rust.

Recommended development environment:

```text
Python 3.12+
Rust stable
Maturin
PyO3
```

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install Maturin:

```bash
python -m pip install maturin
```

Build and install Kurd in development mode:

```bash
maturin develop --release
```

Run the test suite:

```bash
python -m pytest -q
```

Build a release wheel:

```bash
maturin build --release
```

## Project Structure

```text
kurd-mcp/
├── kurd/
│   ├── __init__.py
│   ├── router.py
│   └── _kurd.*
│
├── src/
│   └── lib.rs
│
├── tests/
│
├── benchmarks/
│
├── Cargo.toml
├── pyproject.toml
├── README.md
└── LICENSE
```

## Benchmarking

Kurd includes benchmark work focused on comparing the Rust preprocessing path against equivalent pure-Python processing.

Example local `pyperf` measurements:

```text
Python: 5.92 us
Rust:   2.26 us
Speedup: 2.62x
```

These numbers are preliminary local microbenchmarks and are not production performance guarantees.

Performance may vary depending on:

- CPU architecture
- Python version
- operating system
- payload size
- batch size
- system load
- compiler configuration
- Rust optimization level

Future benchmarks will include reproducible cross-platform measurements.

## Roadmap

Planned areas of development include:

- MCP-native routing
- Streamable HTTP transport
- connection management
- request routing
- concurrency control
- backpressure
- timeouts and cancellation
- upstream health checks
- observability
- structured error handling
- improved Python type support
- cross-platform wheels
- automated CI/CD releases
- benchmark automation
- Linux, macOS, and Windows performance testing

## Design Goals

Kurd is being designed around several core principles:

### Python Ergonomics

Developers should interact with Kurd through a simple and familiar Python API.

### Rust Performance

Performance-sensitive protocol processing should be handled by native Rust code whenever doing so provides a measurable benefit.

### Minimal Python/Rust Boundary Overhead

Data should cross the Python/Rust boundary only when necessary.

Parsing data in Rust and immediately serializing it back into JSON for Python to parse again should be avoided.

### MCP-Native Architecture

Kurd is intended to evolve into an MCP-aware gateway rather than remain only a generic JSON-RPC router.

### Measurable Performance

Performance claims should be supported by reproducible benchmarks rather than theoretical assumptions.

## Technology Stack

Kurd currently uses:

- Python
- Rust
- PyO3
- Maturin
- Tokio
- Serde
- serde_json
- pytest
- pyperf

## Python API

The public API is intended to remain Python-friendly.

Example:

```python
from kurd import Router

router = Router()


@router.tool()
async def multiply(a: int, b: int):
    return a * b
```

Requests can then be dispatched through the router:

```python
response = await router.dispatch(
    """
    {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "multiply",
        "params": {
            "a": 4,
            "b": 5
        }
    }
    """
)
```

## Error Handling

Kurd currently supports basic JSON-RPC error responses, including:

```text
-32700  Parse error
-32601  Method not found
-32602  Invalid params
-32603  Internal error
```

Error handling will continue to evolve as MCP protocol support becomes more complete.

## Building From Source

Clone the repository:

```bash
git clone https://github.com/sn391/kurd.git
cd kurd
```

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install development dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install maturin pytest pyperf
```

Build the Rust extension:

```bash
maturin develop --release
```

Run tests:

```bash
python -m pytest -q
```

Build a distributable wheel:

```bash
maturin build --release
```

Generated wheels are placed under:

```text
target/wheels/
```

## Testing

Run the complete test suite with:

```bash
python -m pytest -q
```

Current tests cover areas such as:

- package import
- Rust extension availability
- valid JSON parsing
- invalid JSON parsing
- parameter extraction
- router dispatch
- method-not-found handling
- invalid parameters
- internal errors

Additional integration and transport tests will be added as the project develops.

## Package Layout

Kurd is a mixed Python/Rust package.

The Python package exposes the developer-facing API:

```text
kurd/
├── __init__.py
├── router.py
└── _kurd.*
```

The native extension is implemented in Rust:

```text
src/
└── lib.rs
```

The private native module is exposed internally as:

```python
kurd._kurd
```

Users should generally interact with the public API exposed by:

```python
import kurd
```

rather than depending directly on private native implementation details.

## Compatibility

The project is currently being developed and tested primarily with:

```text
Python 3.12
Windows x86-64
Rust stable
```

Support for additional Python versions, operating systems, and architectures will be added through automated wheel builds.

## Contributing

Kurd is currently in an early development phase.

Contribution guidelines will be added as the public API and architecture stabilize.

For bugs, ideas, and technical discussions, use the GitHub issue tracker:

```text
https://github.com/sn391/kurd/issues
```

## Security

Kurd is not yet considered production-ready.

If you discover a security issue, avoid publishing sensitive exploit details in a public issue.

A dedicated security policy and private vulnerability reporting process will be added as the project approaches production readiness.

## License

Kurd is released under the MIT License.

## Name

The name **Kurd** honors Kurdish identity and heritage.
Bezhi Kurd u Kurdistan