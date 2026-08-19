use pyo3::prelude::*;
use serde_json::Value;
use tokio::net::TcpListener;
use std::time::{Duration, Instant};
use std::net::IpAddr;
use rand::Rng;
use std::collections::HashMap;
use axum::{
    body::Bytes,
    extract::DefaultBodyLimit,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router as AxumRouter,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use once_cell::sync::Lazy;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

static UPSTREAM_REGISTRY: Lazy<RwLock<HashMap<String, String>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));


struct HttpServerControl {
    addr: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

static HTTP_SERVER_CONTROL: Lazy<Mutex<Option<HttpServerControl>>> =
    Lazy::new(|| Mutex::new(None));


static HTTP_BEARER_TOKEN: Lazy<RwLock<Option<String>>> =
    Lazy::new(|| RwLock::new(None));

static ALLOW_PRIVATE_UPSTREAMS: AtomicBool = AtomicBool::new(true);
static UPSTREAM_TIMEOUT_MS: AtomicU64 = AtomicU64::new(30_000);

const MAX_MCP_BODY_BYTES: usize = 1024 * 1024;

// Production backpressure defaults. All are configurable from Python.
static GLOBAL_CONCURRENCY_LIMIT: AtomicU64 = AtomicU64::new(512);
static UPSTREAM_CONCURRENCY_LIMIT: AtomicU64 = AtomicU64::new(64);
static PYTHON_CONCURRENCY_LIMIT: AtomicU64 = AtomicU64::new(64);
static REQUEST_LOGGING_ENABLED: AtomicBool = AtomicBool::new(false);

static GLOBAL_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GLOBAL_PEAK_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TOTAL_HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);
static COMPLETED_HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);
static REJECTED_HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TOTAL_HTTP_LATENCY_MS: AtomicU64 = AtomicU64::new(0);
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

static PYTHON_ACTIVE_CALLS: AtomicU64 = AtomicU64::new(0);
static PYTHON_PEAK_ACTIVE_CALLS: AtomicU64 = AtomicU64::new(0);
static PYTHON_REJECTIONS: AtomicU64 = AtomicU64::new(0);

static UPSTREAM_ACTIVE_CALLS: Lazy<Mutex<HashMap<String, u64>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static UPSTREAM_PEAK_CALLS: Lazy<Mutex<HashMap<String, u64>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static UPSTREAM_REJECTIONS: AtomicU64 = AtomicU64::new(0);

const OVERLOAD_ERROR_CODE: i64 = -32029;

struct AtomicPermit {
    counter: &'static AtomicU64,
}

impl Drop for AtomicPermit {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

fn update_peak(peak: &'static AtomicU64, value: u64) {
    let mut current = peak.load(Ordering::Acquire);
    while value > current {
        match peak.compare_exchange_weak(
            current,
            value,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn try_acquire_atomic(
    counter: &'static AtomicU64,
    limit: u64,
    peak: &'static AtomicU64,
) -> Option<AtomicPermit> {
    loop {
        let current = counter.load(Ordering::Acquire);
        if current >= limit {
            return None;
        }

        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                update_peak(peak, current + 1);
                return Some(AtomicPermit { counter });
            }
            Err(_) => continue,
        }
    }
}

struct UpstreamPermit {
    name: String,
}

impl Drop for UpstreamPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = UPSTREAM_ACTIVE_CALLS.lock() {
            if let Some(value) = active.get_mut(&self.name) {
                *value = value.saturating_sub(1);
                if *value == 0 {
                    active.remove(&self.name);
                }
            }
        }
    }
}

fn try_acquire_upstream(name: &str) -> Option<UpstreamPermit> {
    let limit = UPSTREAM_CONCURRENCY_LIMIT.load(Ordering::Acquire);
    let current = {
        let mut active = UPSTREAM_ACTIVE_CALLS.lock().ok()?;
        let value = active.entry(name.to_string()).or_insert(0);
        if *value >= limit {
            return None;
        }
        *value += 1;
        *value
    };

    if let Ok(mut peaks) = UPSTREAM_PEAK_CALLS.lock() {
        let peak = peaks.entry(name.to_string()).or_insert(0);
        if current > *peak {
            *peak = current;
        }
    }

    Some(UpstreamPermit {
        name: name.to_string(),
    })
}

fn request_trace_id(headers: &HeaderMap) -> String {
    if let Some(value) = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 128)
    {
        return value.to_string();
    }

    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1;
    format!("kurd-{sequence}")
}

fn log_http_request(
    request_id: &str,
    method: Option<&str>,
    status: StatusCode,
    latency_ms: u64,
    body_bytes: usize,
) {
    if !REQUEST_LOGGING_ENABLED.load(Ordering::Relaxed) {
        return;
    }

    eprintln!(
        "{}",
        serde_json::json!({
            "event": "kurd.http.request",
            "requestId": request_id,
            "method": method,
            "status": status.as_u16(),
            "latencyMs": latency_ms,
            "bodyBytes": body_bytes
        })
    );
}

static HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(32)
        .tcp_keepalive(std::time::Duration::from_secs(60))
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("failed to build Kurd HTTP client")
});

#[derive(Clone, Copy)]
struct CircuitState {
    failures: u32,
    opened_at: Option<Instant>,
}

static CIRCUIT_BREAKERS: Lazy<RwLock<HashMap<String, CircuitState>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

const CIRCUIT_FAILURE_THRESHOLD: u32 = 5;
const CIRCUIT_RESET_TIMEOUT: Duration = Duration::from_secs(30);
const UPSTREAM_MAX_RETRIES: usize = 3;

#[derive(Clone, Default)]
struct UpstreamMetrics {
    requests: u64,
    successes: u64,
    failures: u64,
    retries: u64,
    total_latency_ms: u128,
    last_latency_ms: u128,
}

static UPSTREAM_METRICS: Lazy<RwLock<HashMap<String, UpstreamMetrics>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));


#[derive(Clone)]
struct ToolsCache {
    tools: Vec<Value>,
    created_at: Instant,
}

static TOOLS_CACHE: Lazy<RwLock<Option<ToolsCache>>> =
    Lazy::new(|| RwLock::new(None));

#[derive(Clone, Copy, Default)]
struct ToolsCacheMetrics {
    hits: u64,
    misses: u64,
    invalidations: u64,
}

static TOOLS_CACHE_METRICS: Lazy<RwLock<ToolsCacheMetrics>> =
    Lazy::new(|| RwLock::new(ToolsCacheMetrics::default()));

const TOOLS_CACHE_TTL: Duration = Duration::from_secs(30);

const MCP_PROTOCOL_VERSION: &str = "2026-07-28";
const MCP_HEADER_MISMATCH: i64 = -32020;
const MCP_UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;
const MCP_LIST_TTL_MS: u64 = 30_000;
const MCP_CACHE_SCOPE: &str = "public";


fn content_type_is_json(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(|mime| {
            let mime = mime.trim().to_ascii_lowercase();
            mime == "application/json" || mime.ends_with("+json")
        })
        .unwrap_or(false)
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();

    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0_u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }

    diff == 0
}

fn authorize_mcp_request(headers: &HeaderMap) -> bool {
    let configured = HTTP_BEARER_TOKEN
        .read()
        .ok()
        .and_then(|token| token.clone());

    let Some(expected) = configured else {
        return true;
    };

    let Some(header) = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    let Some(provided) = header.strip_prefix("Bearer ") else {
        return false;
    };

    constant_time_eq(provided, &expected)
}

fn is_private_or_local_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }

    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };

    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
        }
    }
}

fn validate_upstream_url(url: &str) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| "Upstream URL is invalid".to_string())?;

    match parsed.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(
                "Upstream URL must use http or https".to_string()
            );
        }
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(
            "Upstream URL must not contain embedded credentials".to_string()
        );
    }

    if parsed.fragment().is_some() {
        return Err(
            "Upstream URL must not contain a fragment".to_string()
        );
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "Upstream URL must contain a host".to_string())?;

    if !ALLOW_PRIVATE_UPSTREAMS.load(Ordering::Relaxed)
        && is_private_or_local_host(host)
    {
        return Err(
            "Private, loopback, link-local, and unspecified upstream hosts are disabled"
                .to_string()
        );
    }

    Ok(())
}

fn sanitized_upstream_error(upstream_name: &str) -> Value {
    serde_json::json!({
        "upstream": upstream_name,
        "reason": "request_failed"
    })
}

fn request_meta<'a>(parsed: &'a Value) -> Option<&'a serde_json::Map<String, Value>> {
    parsed
        .get("params")
        .and_then(|value| value.as_object())
        .and_then(|params| params.get("_meta"))
        .and_then(|value| value.as_object())
}

fn body_protocol_version(parsed: &Value) -> Option<&str> {
    request_meta(parsed)
        .and_then(|meta| meta.get("io.modelcontextprotocol/protocolVersion"))
        .and_then(|value| value.as_str())
}

fn jsonrpc_error(
    status: StatusCode,
    id: Value,
    code: i64,
    message: &str,
    data: Option<Value>,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let mut error = serde_json::json!({
        "code": code,
        "message": message
    });

    if let Some(data) = data {
        error["data"] = data;
    }

    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error
    });

    (
        status,
        [("content-type", "application/json")],
        response.to_string(),
    )
}

fn validate_modern_http_request(
    headers: &HeaderMap,
    parsed: &Value,
    method: &str,
    request_id: &Value,
) -> Result<(), (StatusCode, [(&'static str, &'static str); 1], String)> {
    let header_protocol = headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok());

    let body_protocol = body_protocol_version(parsed);

    let requested_protocol = body_protocol.or(header_protocol);

    if let Some(requested) = requested_protocol {
        if requested != MCP_PROTOCOL_VERSION {
            return Err(jsonrpc_error(
                StatusCode::BAD_REQUEST,
                request_id.clone(),
                MCP_UNSUPPORTED_PROTOCOL_VERSION,
                "Unsupported protocol version",
                Some(serde_json::json!({
                    "requested": requested,
                    "supported": [MCP_PROTOCOL_VERSION]
                })),
            ));
        }
    }

    let is_modern = header_protocol == Some(MCP_PROTOCOL_VERSION)
        || body_protocol == Some(MCP_PROTOCOL_VERSION);

    if !is_modern {
        return Ok(());
    }

    let Some(header_method) = headers
        .get("mcp-method")
        .and_then(|value| value.to_str().ok())
    else {
        return Err(jsonrpc_error(
            StatusCode::BAD_REQUEST,
            request_id.clone(),
            MCP_HEADER_MISMATCH,
            "Missing Mcp-Method header",
            None,
        ));
    };

    if header_method != method {
        return Err(jsonrpc_error(
            StatusCode::BAD_REQUEST,
            request_id.clone(),
            MCP_HEADER_MISMATCH,
            "Mcp-Method header does not match JSON-RPC method",
            Some(serde_json::json!({
                "header": header_method,
                "body": method
            })),
        ));
    }

    if method == "tools/call" {
        let body_name = parsed
            .get("params")
            .and_then(|value| value.as_object())
            .and_then(|params| params.get("name"))
            .and_then(|value| value.as_str());

        let header_name = headers
            .get("mcp-name")
            .and_then(|value| value.to_str().ok());

        match (header_name, body_name) {
            (Some(header_name), Some(body_name)) if header_name == body_name => {}
            (Some(header_name), Some(body_name)) => {
                return Err(jsonrpc_error(
                    StatusCode::BAD_REQUEST,
                    request_id.clone(),
                    MCP_HEADER_MISMATCH,
                    "Mcp-Name header does not match tool name",
                    Some(serde_json::json!({
                        "header": header_name,
                        "body": body_name
                    })),
                ));
            }
            _ => {
                return Err(jsonrpc_error(
                    StatusCode::BAD_REQUEST,
                    request_id.clone(),
                    MCP_HEADER_MISMATCH,
                    "Missing Mcp-Name header",
                    None,
                ));
            }
        }
    }

    Ok(())
}


async fn list_upstream_tools() -> Vec<Value> {
    if let Ok(cache) = TOOLS_CACHE.read() {
        if let Some(cache) = cache.as_ref() {
            if cache.created_at.elapsed() < TOOLS_CACHE_TTL {
                if let Ok(mut metrics) = TOOLS_CACHE_METRICS.write() {
                    metrics.hits += 1;
                }

                return cache.tools.clone();
            }
        }
    }

    if let Ok(mut metrics) = TOOLS_CACHE_METRICS.write() {
        metrics.misses += 1;
    }

    let upstreams: Vec<(String, String)> = match UPSTREAM_REGISTRY.read() {
        Ok(registry) => registry
            .iter()
            .map(|(name, url)| (name.clone(), url.clone()))
            .collect(),

        Err(_) => return Vec::new(),
    };

    let mut tasks = tokio::task::JoinSet::new();

    for (upstream_name, upstream_url) in upstreams {
        tasks.spawn(async move {
            let payload = serde_json::json!({
                "jsonrpc": "2.0",
                "id": format!("kurd-tools-list-{upstream_name}"),
                "method": "tools/list",
                "params": {
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL_VERSION,
                        "io.modelcontextprotocol/clientInfo": {
                            "name": "kurd",
                            "version": env!("CARGO_PKG_VERSION")
                        },
                        "io.modelcontextprotocol/clientCapabilities": {}
                    }
                }
            });

            let timeout = Duration::from_millis(
                UPSTREAM_TIMEOUT_MS.load(Ordering::Relaxed)
            );

            let response = match HTTP_CLIENT
                .post(&upstream_url)
                .header("MCP-Protocol-Version", MCP_PROTOCOL_VERSION)
                .header("Mcp-Method", "tools/list")
                .timeout(timeout)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => response,
                _ => return Vec::<Value>::new(),
            };

            let body: Value = match response.json().await {
                Ok(value) => value,
                Err(_) => return Vec::<Value>::new(),
            };

            let Some(tools) = body
                .get("result")
                .and_then(|result| result.get("tools"))
                .and_then(|tools| tools.as_array())
            else {
                return Vec::<Value>::new();
            };

            tools
                .iter()
                .filter_map(|tool| {
                    let remote_name = tool
                        .get("name")
                        .and_then(|value| value.as_str())?;

                    let mut tool = tool.clone();

                    if let Some(object) = tool.as_object_mut() {
                        object.insert(
                            "name".to_string(),
                            Value::String(
                                format!("{upstream_name}.{remote_name}")
                            ),
                        );
                    }

                    Some(tool)
                })
                .collect::<Vec<_>>()
        });
    }

    let mut collected_tools = Vec::new();

    while let Some(result) = tasks.join_next().await {
        if let Ok(mut tools) = result {
            collected_tools.append(&mut tools);
        }
    }

    if let Ok(mut cache) = TOOLS_CACHE.write() {
        *cache = Some(ToolsCache {
            tools: collected_tools.clone(),
            created_at: Instant::now(),
        });
    }

    collected_tools
}


async fn forward_to_upstream(
    upstream_name: &str,
    payload: &Value,
) -> Result<Value, String> {
    let upstream_url = {
        let upstreams = UPSTREAM_REGISTRY
            .read()
            .map_err(|_| "Upstream registry lock poisoned".to_string())?;

        upstreams
            .get(upstream_name)
            .cloned()
            .ok_or_else(|| format!("Upstream not found: {upstream_name}"))?
    };

    let _upstream_permit = match try_acquire_upstream(upstream_name) {
        Some(permit) => permit,
        None => {
            UPSTREAM_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            return Err("overloaded".to_string());
        }
    };

    let started_at = Instant::now();

    if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
        metrics
            .entry(upstream_name.to_string())
            .or_default()
            .requests += 1;
    }

    {
        let mut breakers = CIRCUIT_BREAKERS
            .write()
            .map_err(|_| "Circuit breaker lock poisoned".to_string())?;

        let state = breakers
            .entry(upstream_name.to_string())
            .or_insert(CircuitState {
                failures: 0,
                opened_at: None,
            });

        if let Some(opened_at) = state.opened_at {
            if opened_at.elapsed() < CIRCUIT_RESET_TIMEOUT {
                if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
                    metrics
                        .entry(upstream_name.to_string())
                        .or_default()
                        .failures += 1;
                }

                return Err(format!(
                    "Circuit open for upstream: {upstream_name}"
                ));
            }

            state.failures = 0;
            state.opened_at = None;
        }
    }

    let mut last_error = None;

    for attempt in 0..UPSTREAM_MAX_RETRIES {
        let method = payload
            .get("method")
            .and_then(|value| value.as_str())
            .unwrap_or("");

        let timeout = Duration::from_millis(
            UPSTREAM_TIMEOUT_MS.load(Ordering::Relaxed)
        );

        let mut request = HTTP_CLIENT
            .post(&upstream_url)
            .header("MCP-Protocol-Version", MCP_PROTOCOL_VERSION)
            .header("Mcp-Method", method)
            .timeout(timeout);

        if method == "tools/call" {
            if let Some(name) = payload
                .get("params")
                .and_then(|value| value.as_object())
                .and_then(|params| params.get("name"))
                .and_then(|value| value.as_str())
            {
                request = request.header("Mcp-Name", name);
            }
        }

        let response = request
            .json(payload)
            .send()
            .await;

        match response {
            Ok(response) => {
                let status = response.status();

                if status.is_success() {
                    let value = response
                        .json::<Value>()
                        .await
                        .map_err(|e| e.to_string())?;

                    if let Ok(mut breakers) = CIRCUIT_BREAKERS.write() {
                        breakers.insert(
                            upstream_name.to_string(),
                            CircuitState {
                                failures: 0,
                                opened_at: None,
                            },
                        );
                    }

                    let latency_ms = started_at.elapsed().as_millis();
                    if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
                        let entry = metrics
                            .entry(upstream_name.to_string())
                            .or_default();
                        entry.successes += 1;
                        entry.total_latency_ms += latency_ms;
                        entry.last_latency_ms = latency_ms;
                    }

                    return Ok(value);
                }

                if !status.is_server_error() && status.as_u16() != 429 {
                    return Err(format!(
                        "Upstream returned HTTP {status}"
                    ));
                }

                last_error = Some(format!(
                    "Upstream returned HTTP {status}"
                ));
            }

            Err(error) => {
                last_error = Some(error.to_string());
            }
        }

        if attempt + 1 < UPSTREAM_MAX_RETRIES {
            if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
                metrics
                    .entry(upstream_name.to_string())
                    .or_default()
                    .retries += 1;
            }

            let base_ms = 50_u64 * (1_u64 << attempt);

            let jitter_ms = rand::rng()
                .random_range(0..=25_u64);

            tokio::time::sleep(
                Duration::from_millis(base_ms + jitter_ms)
            )
            .await;
        }
    }

    {
        let mut breakers = CIRCUIT_BREAKERS
            .write()
            .map_err(|_| "Circuit breaker lock poisoned".to_string())?;

        let state = breakers
            .entry(upstream_name.to_string())
            .or_insert(CircuitState {
                failures: 0,
                opened_at: None,
            });

        state.failures += 1;

        if state.failures >= CIRCUIT_FAILURE_THRESHOLD {
            state.opened_at = Some(Instant::now());
        }
    }

    let latency_ms = started_at.elapsed().as_millis();
    if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
        let entry = metrics
            .entry(upstream_name.to_string())
            .or_default();
        entry.failures += 1;
        entry.total_latency_ms += latency_ms;
        entry.last_latency_ms = latency_ms;
    }

    Err(last_error.unwrap_or_else(|| {
        format!("Unknown upstream error: {upstream_name}")
    }))
}


fn invalidate_tools_cache() {
    if let Ok(mut cache) = TOOLS_CACHE.write() {
        *cache = None;
    }

    if let Ok(mut metrics) = TOOLS_CACHE_METRICS.write() {
        metrics.invalidations += 1;
    }
}

#[pyfunction]
fn clear_tools_cache() -> PyResult<()> {
    invalidate_tools_cache();
    Ok(())
}

#[pyfunction]
fn unregister_upstream(name: String) -> PyResult<bool> {
    let removed = {
        let mut upstreams = UPSTREAM_REGISTRY
            .write()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Upstream registry lock poisoned"
                )
            })?;

        upstreams.remove(&name).is_some()
    };

    if removed {
        if let Ok(mut breakers) = CIRCUIT_BREAKERS.write() {
            breakers.remove(&name);
        }

        if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
            metrics.remove(&name);
        }

        invalidate_tools_cache();
    }

    Ok(removed)
}


#[pyfunction]
fn set_runtime_limits(
    global_concurrency: u64,
    upstream_concurrency: u64,
    python_concurrency: u64,
) -> PyResult<()> {
    if global_concurrency == 0 || upstream_concurrency == 0 || python_concurrency == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Concurrency limits must be greater than zero",
        ));
    }

    GLOBAL_CONCURRENCY_LIMIT.store(global_concurrency, Ordering::Release);
    UPSTREAM_CONCURRENCY_LIMIT.store(upstream_concurrency, Ordering::Release);
    PYTHON_CONCURRENCY_LIMIT.store(python_concurrency, Ordering::Release);
    Ok(())
}

#[pyfunction]
fn set_request_logging(enabled: bool) -> PyResult<()> {
    REQUEST_LOGGING_ENABLED.store(enabled, Ordering::Release);
    Ok(())
}

#[pyfunction]
fn runtime_status() -> PyResult<(u64, u64, u64, u64, u64, u64, u64, u64, u64, bool)> {
    Ok((
        GLOBAL_CONCURRENCY_LIMIT.load(Ordering::Acquire),
        UPSTREAM_CONCURRENCY_LIMIT.load(Ordering::Acquire),
        PYTHON_CONCURRENCY_LIMIT.load(Ordering::Acquire),
        GLOBAL_ACTIVE_REQUESTS.load(Ordering::Acquire),
        GLOBAL_PEAK_ACTIVE_REQUESTS.load(Ordering::Acquire),
        TOTAL_HTTP_REQUESTS.load(Ordering::Acquire),
        COMPLETED_HTTP_REQUESTS.load(Ordering::Acquire),
        REJECTED_HTTP_REQUESTS.load(Ordering::Acquire),
        PYTHON_REJECTIONS.load(Ordering::Acquire),
        REQUEST_LOGGING_ENABLED.load(Ordering::Acquire),
    ))
}

#[pyfunction]
fn set_http_bearer_token(token: String) -> PyResult<()> {
    if token.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Bearer token cannot be empty",
        ));
    }

    let mut configured = HTTP_BEARER_TOKEN
        .write()
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "HTTP bearer token lock poisoned"
            )
        })?;

    *configured = Some(token);
    Ok(())
}

#[pyfunction]
fn clear_http_bearer_token() -> PyResult<()> {
    let mut configured = HTTP_BEARER_TOKEN
        .write()
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "HTTP bearer token lock poisoned"
            )
        })?;

    *configured = None;
    Ok(())
}

#[pyfunction]
fn set_allow_private_upstreams(allow: bool) -> PyResult<()> {
    ALLOW_PRIVATE_UPSTREAMS.store(allow, Ordering::Relaxed);
    Ok(())
}

#[pyfunction]
fn set_upstream_timeout_ms(timeout_ms: u64) -> PyResult<()> {
    if timeout_ms == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Upstream timeout must be greater than zero",
        ));
    }

    UPSTREAM_TIMEOUT_MS.store(timeout_ms, Ordering::Relaxed);
    Ok(())
}

#[pyfunction]
fn security_status() -> PyResult<(bool, bool, u64, usize)> {
    let auth_enabled = HTTP_BEARER_TOKEN
        .read()
        .map(|token| token.is_some())
        .unwrap_or(false);

    Ok((
        auth_enabled,
        ALLOW_PRIVATE_UPSTREAMS.load(Ordering::Relaxed),
        UPSTREAM_TIMEOUT_MS.load(Ordering::Relaxed),
        MAX_MCP_BODY_BYTES,
    ))
}

#[pyfunction]
fn register_upstream(
    name: String,
    url: String,
) -> PyResult<()> {
    if name.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Upstream name cannot be empty",
        ));
    }

    validate_upstream_url(&url)
        .map_err(pyo3::exceptions::PyValueError::new_err)?;

    let mut upstreams = UPSTREAM_REGISTRY
        .write()
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Upstream registry lock poisoned"
            )
        })?;

    upstreams.insert(name, url);

    drop(upstreams);

    invalidate_tools_cache();

    Ok(())
}


#[pyfunction]
fn init_python_async_runtime(py: Python<'_>) -> PyResult<()> {
    {
        let runtime = PY_ASYNC_RUNTIME
            .read()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Async runtime lock poisoned"
                )
            })?;

        if runtime.is_some() {
            return Ok(());
        }
    }

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
        .route("/status", get(status))
        .route("/mcp", post(mcp_root))
        .layer(DefaultBodyLimit::max(MAX_MCP_BODY_BYTES))
}

async fn run_http_server(
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let app = build_http_router();

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        })
        .await?;

    Ok(())
}

fn clear_http_server_control() {
    if let Ok(mut control) = HTTP_SERVER_CONTROL.lock() {
        *control = None;
    }
}

#[pyfunction]
fn start_http_gateway(py: Python<'_>, addr: String) -> PyResult<()> {
    if addr.trim().is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "HTTP gateway address cannot be empty",
        ));
    }

    py.detach(|| {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    e.to_string()
                )
            })?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        {
            let mut control = HTTP_SERVER_CONTROL
                .lock()
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "HTTP server control lock poisoned"
                    )
                })?;

            if let Some(existing) = control.as_ref() {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    format!(
                        "Kurd HTTP gateway is already running on {}",
                        existing.addr
                    )
                ));
            }

            *control = Some(HttpServerControl {
                addr: addr.clone(),
                shutdown_tx: Some(shutdown_tx),
            });
        }

        let result = runtime
            .block_on(run_http_server(&addr, shutdown_rx))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    e.to_string()
                )
            });

        clear_http_server_control();

        result
    })
}

#[pyfunction]
fn stop_http_gateway() -> PyResult<bool> {
    let shutdown_tx = {
        let mut control = HTTP_SERVER_CONTROL
            .lock()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "HTTP server control lock poisoned"
                )
            })?;

        let Some(server) = control.as_mut() else {
            return Ok(false);
        };

        server.shutdown_tx.take()
    };

    let Some(shutdown_tx) = shutdown_tx else {
        return Ok(false);
    };

    shutdown_tx
        .send(())
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "HTTP gateway shutdown signal could not be delivered"
            )
        })?;

    Ok(true)
}

#[pyfunction]
fn http_gateway_status() -> PyResult<(bool, Option<String>)> {
    let control = HTTP_SERVER_CONTROL
        .lock()
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "HTTP server control lock poisoned"
            )
        })?;

    match control.as_ref() {
        Some(server) => Ok((true, Some(server.addr.clone()))),
        None => Ok((false, None)),
    }
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
    m.add_function(wrap_pyfunction!(stop_http_gateway, m)?)?;
    m.add_function(wrap_pyfunction!(http_gateway_status, m)?)?;
    m.add_function(wrap_pyfunction!(register_tool, m)?)?;
    m.add_function(wrap_pyfunction!(init_python_async_runtime, m)?)?;
    m.add_function(wrap_pyfunction!(register_upstream, m)?)?;
    m.add_function(wrap_pyfunction!(unregister_upstream, m)?)?;
    m.add_function(wrap_pyfunction!(clear_tools_cache, m)?)?;
    m.add_function(wrap_pyfunction!(set_http_bearer_token, m)?)?;
    m.add_function(wrap_pyfunction!(clear_http_bearer_token, m)?)?;
    m.add_function(wrap_pyfunction!(set_allow_private_upstreams, m)?)?;
    m.add_function(wrap_pyfunction!(set_upstream_timeout_ms, m)?)?;
    m.add_function(wrap_pyfunction!(security_status, m)?)?;
    m.add_function(wrap_pyfunction!(set_runtime_limits, m)?)?;
    m.add_function(wrap_pyfunction!(set_request_logging, m)?)?;
    m.add_function(wrap_pyfunction!(runtime_status, m)?)?;
    
    Ok(())
}


async fn health() -> &'static str {
    "Kurd MCP Gateway"
}

async fn status() -> impl IntoResponse {
    let upstreams = UPSTREAM_REGISTRY
        .read()
        .map(|registry| registry.clone())
        .unwrap_or_default();

    let metrics = UPSTREAM_METRICS
        .read()
        .map(|registry| registry.clone())
        .unwrap_or_default();

    let breakers = CIRCUIT_BREAKERS
        .read()
        .map(|registry| registry.clone())
        .unwrap_or_default();

    let mut upstream_status = serde_json::Map::new();

    for (name, url) in upstreams {
        let metric = metrics.get(&name).cloned().unwrap_or_default();
        let breaker = breakers.get(&name).copied().unwrap_or(CircuitState {
            failures: 0,
            opened_at: None,
        });

        let circuit = match breaker.opened_at {
            Some(opened_at) if opened_at.elapsed() < CIRCUIT_RESET_TIMEOUT => "open",
            _ => "closed",
        };

        let average_latency_ms = if metric.successes + metric.failures > 0 {
            metric.total_latency_ms as f64
                / (metric.successes + metric.failures) as f64
        } else {
            0.0
        };

        upstream_status.insert(
            name,
            serde_json::json!({
                "url": url,
                "requests": metric.requests,
                "successes": metric.successes,
                "failures": metric.failures,
                "retries": metric.retries,
                "lastLatencyMs": metric.last_latency_ms,
                "averageLatencyMs": average_latency_ms,
                "circuit": circuit,
                "circuitFailures": breaker.failures
            }),
        );
    }

    let cache_metrics = TOOLS_CACHE_METRICS
        .read()
        .map(|metrics| *metrics)
        .unwrap_or_default();

    let cache_status = TOOLS_CACHE
        .read()
        .ok()
        .and_then(|cache| {
            cache.as_ref().map(|entry| {
                serde_json::json!({
                    "cached": entry.created_at.elapsed() < TOOLS_CACHE_TTL,
                    "ageMs": entry.created_at.elapsed().as_millis(),
                    "ttlMs": TOOLS_CACHE_TTL.as_millis(),
                    "toolCount": entry.tools.len(),
                    "hits": cache_metrics.hits,
                    "misses": cache_metrics.misses,
                    "invalidations": cache_metrics.invalidations
                })
            })
        })
        .unwrap_or_else(|| {
            serde_json::json!({
                "cached": false,
                "ageMs": 0,
                "ttlMs": TOOLS_CACHE_TTL.as_millis(),
                "toolCount": 0,
                "hits": cache_metrics.hits,
                "misses": cache_metrics.misses,
                "invalidations": cache_metrics.invalidations
            })
        });

    let (http_running, http_addr) = HTTP_SERVER_CONTROL
        .lock()
        .map(|control| {
            control
                .as_ref()
                .map(|server| (true, Some(server.addr.clone())))
                .unwrap_or((false, None))
        })
        .unwrap_or((false, None));

    let auth_enabled = HTTP_BEARER_TOKEN
        .read()
        .map(|token| token.is_some())
        .unwrap_or(false);

    let upstream_active = UPSTREAM_ACTIVE_CALLS
        .lock()
        .map(|active| active.clone())
        .unwrap_or_default();

    let upstream_peaks = UPSTREAM_PEAK_CALLS
        .lock()
        .map(|peaks| peaks.clone())
        .unwrap_or_default();

    let mut upstream_concurrency = serde_json::Map::new();
    for name in UPSTREAM_REGISTRY
        .read()
        .map(|registry| registry.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default()
    {
        upstream_concurrency.insert(
            name.clone(),
            serde_json::json!({
                "active": upstream_active.get(&name).copied().unwrap_or(0),
                "peak": upstream_peaks.get(&name).copied().unwrap_or(0)
            }),
        );
    }

    let total_http = TOTAL_HTTP_REQUESTS.load(Ordering::Acquire);
    let completed_http = COMPLETED_HTTP_REQUESTS.load(Ordering::Acquire);
    let total_http_latency_ms = TOTAL_HTTP_LATENCY_MS.load(Ordering::Acquire);
    let average_http_latency_ms = if completed_http > 0 {
        total_http_latency_ms as f64 / completed_http as f64
    } else {
        0.0
    };

    let response = serde_json::json!({
        "name": "kurd",
        "version": env!("CARGO_PKG_VERSION"),
        "status": "ok",
        "http": {
            "running": http_running,
            "address": http_addr
        },
        "security": {
            "authEnabled": auth_enabled,
            "maxMcpBodyBytes": MAX_MCP_BODY_BYTES,
            "allowPrivateUpstreams": ALLOW_PRIVATE_UPSTREAMS.load(Ordering::Relaxed),
            "upstreamTimeoutMs": UPSTREAM_TIMEOUT_MS.load(Ordering::Relaxed)
        },
        "runtime": {
            "globalConcurrencyLimit": GLOBAL_CONCURRENCY_LIMIT.load(Ordering::Acquire),
            "upstreamConcurrencyLimit": UPSTREAM_CONCURRENCY_LIMIT.load(Ordering::Acquire),
            "pythonConcurrencyLimit": PYTHON_CONCURRENCY_LIMIT.load(Ordering::Acquire),
            "activeRequests": GLOBAL_ACTIVE_REQUESTS.load(Ordering::Acquire),
            "peakActiveRequests": GLOBAL_PEAK_ACTIVE_REQUESTS.load(Ordering::Acquire),
            "totalRequests": total_http,
            "completedRequests": completed_http,
            "rejectedRequests": REJECTED_HTTP_REQUESTS.load(Ordering::Acquire),
            "averageLatencyMs": average_http_latency_ms,
            "pythonActiveCalls": PYTHON_ACTIVE_CALLS.load(Ordering::Acquire),
            "pythonPeakActiveCalls": PYTHON_PEAK_ACTIVE_CALLS.load(Ordering::Acquire),
            "pythonRejectedCalls": PYTHON_REJECTIONS.load(Ordering::Acquire),
            "upstreamRejectedCalls": UPSTREAM_REJECTIONS.load(Ordering::Acquire),
            "requestLoggingEnabled": REQUEST_LOGGING_ENABLED.load(Ordering::Acquire),
            "upstreams": upstream_concurrency
        },
        "localTools": TOOL_REGISTRY.read().map(|r| r.len()).unwrap_or(0),
        "upstreamCount": upstream_status.len(),
        "toolsCache": cache_status,
        "upstreams": upstream_status
    });

    (
        StatusCode::OK,
        [("content-type", "application/json")],
        response.to_string(),
    )
}

fn execute_python_tool(
    tool_name: String,
    arguments: Value,
) -> PyResult<String> {
    let callback_result = Python::attach(|py| -> PyResult<Py<PyAny>> {
        let tools = TOOL_REGISTRY
            .read()
            .map_err(|_| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "Tool registry lock poisoned"
                )
            })?;

        let tool = tools
            .get(&tool_name)
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
    })?;

    let is_awaitable = Python::attach(|py| -> PyResult<bool> {
        callback_result
            .bind(py)
            .hasattr("__await__")
    })
    .unwrap_or(false);

    let final_result: Py<PyAny> = if is_awaitable {
        Python::attach(|py| -> PyResult<Py<PyAny>> {
            let asyncio = py.import("asyncio")?;

            let runtime = PY_ASYNC_RUNTIME
                .read()
                .map_err(|_| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "Async runtime lock poisoned"
                    )
                })?;

            let runtime = runtime
                .as_ref()
                .ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(
                        "Python async runtime is not initialized"
                    )
                })?;

            let future = asyncio.call_method1(
                "run_coroutine_threadsafe",
                (
                    callback_result.bind(py),
                    runtime.loop_obj.bind(py),
                ),
            )?;

            // This is intentionally executed from Tokio's blocking pool.
            // Waiting for the Python future must never block an Axum/Tokio
            // async worker thread.
            let result = future.call_method0("result")?;

            Ok(result.unbind())
        })?
    } else {
        callback_result
    };

    Python::attach(|py| -> PyResult<String> {
        let json_module = py.import("json")?;

        json_module
            .call_method1(
                "dumps",
                (final_result.bind(py),),
            )?
            .extract::<String>()
    })
}

async fn mcp_root(headers: HeaderMap, body: Bytes) -> Response {
    let started_at = Instant::now();
    let request_id = request_trace_id(&headers);
    let body_bytes = body.len();
    let method = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("method")
                .and_then(|method| method.as_str())
                .map(str::to_owned)
        });

    TOTAL_HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);

    let global_limit = GLOBAL_CONCURRENCY_LIMIT.load(Ordering::Acquire);
    let _global_permit = match try_acquire_atomic(
        &GLOBAL_ACTIVE_REQUESTS,
        global_limit,
        &GLOBAL_PEAK_ACTIVE_REQUESTS,
    ) {
        Some(permit) => permit,
        None => {
            REJECTED_HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
            let mut response = jsonrpc_error(
                StatusCode::SERVICE_UNAVAILABLE,
                Value::Null,
                OVERLOAD_ERROR_CODE,
                "Gateway overloaded",
                Some(serde_json::json!({
                    "limit": global_limit
                })),
            )
            .into_response();

            if let Ok(value) = HeaderValue::from_str(&request_id) {
                response.headers_mut().insert("x-request-id", value);
            }

            let latency_ms = started_at.elapsed().as_millis() as u64;
            log_http_request(
                &request_id,
                method.as_deref(),
                response.status(),
                latency_ms,
                body_bytes,
            );
            return response;
        }
    };

    let mut response = mcp_root_inner(headers, body).await.into_response();

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }

    let latency_ms = started_at.elapsed().as_millis() as u64;
    COMPLETED_HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
    TOTAL_HTTP_LATENCY_MS.fetch_add(latency_ms, Ordering::Relaxed);
    log_http_request(
        &request_id,
        method.as_deref(),
        response.status(),
        latency_ms,
        body_bytes,
    );

    response
}

async fn mcp_root_inner(headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    if !content_type_is_json(&headers) {
        return jsonrpc_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Value::Null,
            -32600,
            "Content-Type must be application/json",
            None,
        );
    }

    if body.len() > MAX_MCP_BODY_BYTES {
        return jsonrpc_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            Value::Null,
            -32600,
            "Request body too large",
            Some(serde_json::json!({
                "maxBytes": MAX_MCP_BODY_BYTES
            })),
        );
    }

    if !authorize_mcp_request(&headers) {
        return jsonrpc_error(
            StatusCode::UNAUTHORIZED,
            Value::Null,
            -32001,
            "Unauthorized",
            None,
        );
    }

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

    if let Err(error_response) =
        validate_modern_http_request(&headers, &parsed, method, &request_id)
    {
        return error_response;
    }

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
                    MCP_PROTOCOL_VERSION
                ],
                "capabilities": {
                    "tools": {}
                },
                "_meta": {
                    "io.modelcontextprotocol/serverInfo": {
                        "name": "kurd",
                        "version": env!("CARGO_PKG_VERSION")
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
        let mut tools = TOOL_REGISTRY
            .read()
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

        let upstream_tools = list_upstream_tools().await;

        tools.extend(upstream_tools);

        tools.sort_by(|left, right| {
            let left_name = left
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("");

            let right_name = right
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("");

            left_name.cmp(right_name)
        });

        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "resultType": "complete",
                "tools": tools,
                "ttlMs": MCP_LIST_TTL_MS,
                "cacheScope": MCP_CACHE_SCOPE
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

        // -----------------------------------------------------
        // Gateway routing:
        // "github.create_issue" -> upstream "github",
        // forwarded tool name -> "create_issue"
        // -----------------------------------------------------
        if let Some((upstream_name, remote_tool_name)) = tool_name.split_once('.') {
            let has_upstream = UPSTREAM_REGISTRY
                .read()
                .map(|registry| registry.contains_key(upstream_name))
                .unwrap_or(false);

            if has_upstream {
                let mut forwarded_payload = parsed.clone();

                if let Some(forwarded_params) = forwarded_payload
                    .get_mut("params")
                    .and_then(|value| value.as_object_mut())
                {
                    forwarded_params.insert(
                        "name".to_string(),
                        Value::String(remote_tool_name.to_string()),
                    );

                    let meta = forwarded_params
                        .entry("_meta".to_string())
                        .or_insert_with(|| serde_json::json!({}));

                    if let Some(meta) = meta.as_object_mut() {
                        meta.insert(
                            "io.modelcontextprotocol/protocolVersion".to_string(),
                            Value::String(MCP_PROTOCOL_VERSION.to_string()),
                        );

                        meta.entry("io.modelcontextprotocol/clientInfo".to_string())
                            .or_insert_with(|| {
                                serde_json::json!({
                                    "name": "kurd",
                                    "version": env!("CARGO_PKG_VERSION")
                                })
                            });

                        meta.entry("io.modelcontextprotocol/clientCapabilities".to_string())
                            .or_insert_with(|| serde_json::json!({}));
                    }
                }

                match forward_to_upstream(
                    upstream_name,
                    &forwarded_payload,
                )
                .await
                {
                    Ok(response) => {
                        return (
                            StatusCode::OK,
                            [("content-type", "application/json")],
                            response.to_string(),
                        );
                    }

                    Err(error) if error == "overloaded" => {
                        return jsonrpc_error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            request_id,
                            OVERLOAD_ERROR_CODE,
                            "Upstream concurrency limit reached",
                            Some(serde_json::json!({
                                "upstream": upstream_name,
                                "limit": UPSTREAM_CONCURRENCY_LIMIT.load(Ordering::Acquire)
                            })),
                        );
                    }

                    Err(_error) => {
                        let response = serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {
                                "code": -32000,
                                "message": "Upstream request failed",
                                "data": sanitized_upstream_error(upstream_name)
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
        }

        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        let python_limit = PYTHON_CONCURRENCY_LIMIT.load(Ordering::Acquire);
        let _python_permit = match try_acquire_atomic(
            &PYTHON_ACTIVE_CALLS,
            python_limit,
            &PYTHON_PEAK_ACTIVE_CALLS,
        ) {
            Some(permit) => permit,
            None => {
                PYTHON_REJECTIONS.fetch_add(1, Ordering::Relaxed);
                return jsonrpc_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    request_id,
                    OVERLOAD_ERROR_CODE,
                    "Python tool executor overloaded",
                    Some(serde_json::json!({
                        "limit": python_limit
                    })),
                );
            }
        };

        // -----------------------------------------------------
        // Local Python callback execution
        //
        // Python calls can block (especially async callbacks waiting on
        // concurrent.futures.Future.result()). Run the complete Python
        // execution path on Tokio's blocking pool so Axum workers remain
        // available to enforce global backpressure and serve other clients.
        // -----------------------------------------------------
        let tool_name_owned = tool_name.to_string();
        let arguments_owned = arguments.clone();

        let serialized = match tokio::task::spawn_blocking(move || {
            // Hold the Python concurrency permit for the entire callback,
            // including the wait for an async coroutine to complete.
            let _python_permit = _python_permit;
            execute_python_tool(tool_name_owned, arguments_owned)
        })
        .await
        {
            Ok(result) => result,
            Err(error) => Err(
                pyo3::exceptions::PyRuntimeError::new_err(
                    format!("Python tool worker failed: {error}")
                )
            ),
        };

        match serialized {
            Ok(serialized) => {
                let value: Value = serde_json::from_str(&serialized)
                    .unwrap_or(Value::String(serialized));

                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "resultType": "complete",
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
                        "resultType": "complete",
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