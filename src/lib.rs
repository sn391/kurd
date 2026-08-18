use pyo3::prelude::*;
use serde_json::Value;
use tokio::net::TcpListener;
use std::collections::HashMap;
use axum::{
    body::Bytes,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router as AxumRouter,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use once_cell::sync::Lazy;
use std::sync::RwLock;
use pyo3::types::PyAnyMethods;

struct RegisteredTool {
    description: String,
    input_schema: Value,
    callback: Py<PyAny>,
}

struct PythonAsyncRuntime {
    loop_obj: Py<PyAny>,
}

static TOOL_REGISTRY: Lazy<RwLock<HashMap<String, RegisteredTool>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

static PY_ASYNC_RUNTIME: Lazy<RwLock<Option<PythonAsyncRuntime>>> =
    Lazy::new(|| RwLock::new(None));

#[pyfunction]
fn init_python_async_runtime(py: Python<'_>) -> PyResult<()> {
    let asyncio = py.import("asyncio")?;
    let threading = py.import("threading")?;

    let event_loop = asyncio.call_method0("new_event_loop")?;

    let globals = pyo3::types::PyDict::new(py);
    globals.set_item("asyncio", &asyncio)?;
    globals.set_item("loop_obj", &event_loop)?;

    py.run(
        pyo3::ffi::c_str!(
            "
def _kurd_loop_runner():
    asyncio.set_event_loop(loop_obj)
    loop_obj.run_forever()
"
        ),
        Some(&globals),
        None,
    )?;

    let runner = globals
        .get_item("_kurd_loop_runner")?
        .ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Failed to create async loop runner"
            )
        })?;

    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("target", runner)?;
    kwargs.set_item("daemon", true)?;

    let thread = threading.call_method(
        "Thread",
        (),
        Some(&kwargs),
    )?;

    thread.call_method0("start")?;

    let mut runtime = PY_ASYNC_RUNTIME
        .write()
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Async runtime lock poisoned"
            )
        })?;

    *runtime = Some(PythonAsyncRuntime {
        loop_obj: event_loop.unbind(),
    });

    Ok(())
}

#[pyfunction]
fn register_tool(
    py: Python<'_>,
    name: String,
    description: Option<String>,
    input_schema_json: String,
    callback: Py<PyAny>,
) -> PyResult<()> {
    let input_schema: Value = serde_json::from_str(&input_schema_json)
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(
                format!("Invalid input schema: {e}")
            )
        })?;

    let mut tools = TOOL_REGISTRY
        .write()
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Tool registry lock poisoned"
            )
        })?;

    tools.insert(
        name,
        RegisteredTool {
            description: description.unwrap_or_default(),
            input_schema,
            callback: callback.clone_ref(py),
        },
    );

    Ok(())
}


/// High-performance single JSON-RPC validation
#[pyfunction]
fn fast_parse(payload: &str) -> PyResult<(Option<String>, Option<String>, Option<String>)> {
    let parsed: serde_json::Value = serde_json::from_str(payload)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    let method = parsed
        .get("method")
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    let id = parsed
        .get("id")
        .map(|v| v.to_string());
    let params = parsed
        .get("params")
        .map(|v| v.to_string());

    Ok((method, id, params))
}

fn build_http_router() -> AxumRouter {
    AxumRouter::new()
        .route("/health", get(health))
        .route("/mcp", post(mcp_root))
}

async fn run_http_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let app = build_http_router();

    axum::serve(listener, app).await?;

    Ok(())
}

#[pyfunction]
fn start_http_gateway(py: Python<'_>, addr: String) -> PyResult<()> {
    py.detach(|| {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    e.to_string()
                )
            })?;

        runtime
            .block_on(run_http_server(&addr))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    e.to_string()
                )
            })
    })
}

#[pyfunction]
fn fast_parse_batch(
    payloads: Vec<String>,
) -> PyResult<Vec<(Option<String>, Option<String>, Option<String>)>> {
    let mut results = Vec::with_capacity(payloads.len());

    for payload in payloads {
        let parsed: Value = serde_json::from_str(&payload)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let method = parsed
            .get("method")
            .and_then(|v| v.as_str())
            .map(str::to_owned);

        let id = parsed
            .get("id")
            .map(|v| v.to_string());

        let params = parsed
            .get("params")
            .map(|v| v.to_string());

        results.push((method, id, params));
    }

    Ok(results)
}

/// Starts a high-performance TCP Transport Gateway in Rust (manages thousands of connections concurrently)
#[pyfunction]
fn start_tcp_gateway(addr: String, py_callback: Py<PyAny>) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new()?;
    
    rt.block_on(async move {
        let listener = TcpListener::bind(&addr).await.unwrap();
        println!("Rust TCP Transport Gateway running on {}", addr);

        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(val) => val,
                Err(_) => continue,
            };

            let _py_cb = Python::attach(|py| py_callback.clone_ref(py));

            tokio::spawn(async move {
                let mut buf = vec![0; 4096];
                loop {
                    let n = match socket.read(&mut buf).await {
                        Ok(0) => return,
                        Ok(n) => n,
                        Err(_) => return,
                    };

                    let raw_data = String::from_utf8_lossy(&buf[..n]).to_string();
                    
                    // Pre-process and validate rapidly in Rust
                    let parsed_result = match serde_json::from_str::<Value>(&raw_data) {
                        Ok(v) => {
                            serde_json::json!({
                                "status": "success",
                                "method": v.get("method").and_then(|m| m.as_str()).unwrap_or("unknown"),
                                "payload": v
                            }).to_string()
                        }
                        Err(_) => {
                            serde_json::json!({"error": "invalid json"}).to_string()
                        }
                    };

                    // Send back to client over TCP socket
                    if let Err(_) = socket.write_all(parsed_result.as_bytes()).await {
                        break;
                    }
                }
            });
        }
    });

    Ok(())
}

#[pymodule]
fn _kurd(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fast_parse, m)?)?;
    m.add_function(wrap_pyfunction!(fast_parse_batch, m)?)?;
    m.add_function(wrap_pyfunction!(start_tcp_gateway, m)?)?;
    m.add_function(wrap_pyfunction!(start_http_gateway, m)?)?;
    m.add_function(wrap_pyfunction!(register_tool, m)?)?;
    m.add_function(wrap_pyfunction!(init_python_async_runtime, m)?)?;
    Ok(())
}


async fn health() -> &'static str {
    "Kurd MCP Gateway"
}

async fn mcp_root(body: Bytes) -> impl IntoResponse {
    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            let error = serde_json::json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": "Parse error"
                },
                "id": null
            });

            return (
                StatusCode::OK,
                [("content-type", "application/json")],
                error.to_string(),
            );
        }
    };

    let jsonrpc = parsed
        .get("jsonrpc")
        .and_then(|value| value.as_str());

    if jsonrpc != Some("2.0") {
        let error = serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32600,
                "message": "Invalid Request"
            },
            "id": parsed
                .get("id")
                .cloned()
                .unwrap_or(Value::Null)
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            error.to_string(),
        );
    }

    let method = parsed
        .get("method")
        .and_then(|value| value.as_str());

    if method.is_none() {
        let error = serde_json::json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32600,
                "message": "Invalid Request"
            },
            "id": parsed
                .get("id")
                .cloned()
                .unwrap_or(Value::Null)
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            error.to_string(),
        );
    }

    let method = method.unwrap();

    let request_id = parsed
        .get("id")
        .cloned()
        .unwrap_or(Value::Null);

    // ---------------------------------------------------------
    // MCP: server/discover
    // ---------------------------------------------------------
    if method == "server/discover" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "resultType": "complete",
                "supportedVersions": [
                    "2026-07-28"
                ],
                "capabilities": {
                    "tools": {}
                },
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "kurd",
                        "version": "0.1.2"
                    }
                },
                "instructions": "Kurd is a high-performance MCP gateway powered by Rust.",
                "ttlMs": 3600000,
                "cacheScope": "public"
            }
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            response.to_string(),
        );
    }

    // ---------------------------------------------------------
    // MCP / JSON-RPC ping
    // ---------------------------------------------------------
    if method == "ping" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {}
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            response.to_string(),
        );
    }

    // ---------------------------------------------------------
    // MCP: tools/list
    // ---------------------------------------------------------
    if method == "tools/list" {
        let tools = TOOL_REGISTRY
            .read()
            .map_err(|_| ())
            .ok()
            .map(|registry| {
                registry
                    .iter()
                    .map(|(name, tool)| {
                        serde_json::json!({
                            "name": name,
                            "description": tool.description,
                            "inputSchema": tool.input_schema
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": tools
            }
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            response.to_string(),
        );
    }

    // ---------------------------------------------------------
    // MCP: tools/call
    // ---------------------------------------------------------
    if method == "tools/call" {
        let params = parsed
            .get("params")
            .and_then(|value| value.as_object());

        let Some(params) = params else {
            let error = serde_json::json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32602,
                    "message": "Invalid params"
                }
            });

            return (
                StatusCode::OK,
                [("content-type", "application/json")],
                error.to_string(),
            );
        };

        let tool_name = params
            .get("name")
            .and_then(|value| value.as_str());

        let Some(tool_name) = tool_name else {
            let error = serde_json::json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32602,
                    "message": "Invalid params"
                }
            });

            return (
                StatusCode::OK,
                [("content-type", "application/json")],
                error.to_string(),
            );
        };

        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        let callback_result = Python::attach(|py| -> PyResult<Py<PyAny>> {
            let tools = TOOL_REGISTRY
                .read()
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "Tool registry lock poisoned"
                    )
                })?;

            let tool = tools
                .get(tool_name)
                .ok_or_else(|| {
                    pyo3::exceptions::PyKeyError::new_err(
                        format!("Tool not found: {tool_name}")
                    )
                })?;

            let json_module = py.import("json")?;

            let py_args = json_module.call_method1(
                "loads",
                (arguments.to_string(),),
            )?;

            let py_dict = py_args.cast::<pyo3::types::PyDict>()?;

            let callback = tool.callback.bind(py);

            let result = callback.call(
                (),
                Some(py_dict),
            )?;

            Ok(result.unbind())
        });

        let callback_result = match callback_result {
            Ok(value) => value,
            Err(error) => {
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": error.to_string()
                            }
                        ],
                        "isError": true
                    }
                });

                return (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    response.to_string(),
                );
            }
        };

        let is_awaitable = Python::attach(|py| -> PyResult<bool> {
            callback_result
                .bind(py)
                .hasattr("__await__")
        })
        .unwrap_or(false);

        let final_result: PyResult<Py<PyAny>> = if is_awaitable {
            Python::attach(|py| {
                let asyncio = py.import("asyncio")?;

                let result = asyncio.call_method1(
                    "run",
                    (callback_result.bind(py),),
                )?;

                Ok(result.unbind())
            })
        } else {
            Ok(callback_result)
        };

        let serialized = match final_result {
            Ok(value) => {
                Python::attach(|py| -> PyResult<String> {
                    let json_module = py.import("json")?;

                    json_module
                        .call_method1(
                            "dumps",
                            (value.bind(py),),
                        )?
                        .extract::<String>()
                })
            }

            Err(error) => Err(error),
        };

        match serialized {
            Ok(serialized) => {
                let value: Value = serde_json::from_str(&serialized)
                    .unwrap_or(Value::String(serialized));

                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": value.to_string()
                            }
                        ],
                        "isError": false
                    }
                });

                return (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    response.to_string(),
                );
            }

            Err(error) => {
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": error.to_string()
                            }
                        ],
                        "isError": true
                    }
                });

                return (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    response.to_string(),
                );
            }
        }
    }

    // ---------------------------------------------------------
    // Unknown method
    // ---------------------------------------------------------
    let error = serde_json::json!({
        "jsonrpc": "2.0",
        "error": {
            "code": -32601,
            "message": "Method not found"
        },
        "id": request_id
    });

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        error.to_string(),
    )
}