use pyo3::prelude::*;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::net::{IpAddr, SocketAddr};
use std::convert::Infallible;
use axum::extract::ConnectInfo;
use rand::Rng;
use std::collections::HashMap;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Json, Path},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    response::sse::{Event, KeepAlive, Sse},
    routing::{delete, get, post},
    Router as AxumRouter,
};
use tokio::sync::oneshot;
use once_cell::sync::Lazy;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use pyo3::types::PyAnyMethods;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing_subscriber::prelude::*;


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

static ADMIN_TOKEN: Lazy<RwLock<Option<String>>> =
    Lazy::new(|| RwLock::new(None));

static ALLOW_PRIVATE_UPSTREAMS: AtomicBool = AtomicBool::new(true);
static UPSTREAM_TIMEOUT_MS: AtomicU64 = AtomicU64::new(30_000);

const MAX_MCP_BODY_BYTES: usize = 1024 * 1024;
const MAX_UPSTREAM_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

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

// Rate limiting
static RATE_LIMIT_ENABLED: AtomicBool = AtomicBool::new(false);
static RATE_LIMIT_PER_IP_RPS: AtomicU64 = AtomicU64::new(1000);
static RATE_LIMIT_GLOBAL_RPS: AtomicU64 = AtomicU64::new(10000);

static RATE_LIMIT_REQUESTS: Lazy<Mutex<HashMap<String, Vec<u64>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static RATE_LIMIT_LAST_CLEANUP_MS: AtomicU64 = AtomicU64::new(0);

// SSE channels for Streamable HTTP — one sender per connected client.
static SSE_CHANNELS: Lazy<Mutex<HashMap<String, tokio::sync::mpsc::UnboundedSender<String>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

// Session registry: Mcp-Session-Id → client metadata.
#[allow(dead_code)]
struct McpSession {
    client_info: Value,
    client_capabilities: Value,
}

static SESSION_REGISTRY: Lazy<RwLock<HashMap<String, McpSession>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

// Tracing filter reload handle — lets logging/setLevel actually take effect.
type FilterReloadHandle = tracing_subscriber::reload::Handle<
    tracing_subscriber::EnvFilter,
    tracing_subscriber::Registry,
>;
static LOG_FILTER_HANDLE: Lazy<Mutex<Option<FilterReloadHandle>>> =
    Lazy::new(|| Mutex::new(None));

// Task-local W3C traceparent for the current MCP request, used when propagating
// trace context to upstream calls inside the same Tokio task.
tokio::task_local! {
    static TASK_TRACEPARENT: String;
}

// Latency histogram buckets (ms). +Inf is the implicit last slot.
const LATENCY_BUCKETS: [u64; 11] = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];

static LATENCY_HISTOGRAM: Lazy<Vec<AtomicU64>> =
    Lazy::new(|| (0..=LATENCY_BUCKETS.len()).map(|_| AtomicU64::new(0)).collect());

fn record_latency(latency_ms: u64) {
    let bucket = LATENCY_BUCKETS
        .iter()
        .position(|&b| latency_ms <= b)
        .unwrap_or(LATENCY_BUCKETS.len());
    LATENCY_HISTOGRAM[bucket].fetch_add(1, Ordering::Relaxed);
}

static IP_ALLOWLIST: Lazy<RwLock<Option<std::collections::HashSet<IpAddr>>>> =
    Lazy::new(|| RwLock::new(None));

fn check_ip_allowlist(client_ip: &str) -> bool {
    let list = match IP_ALLOWLIST.read() {
        Ok(g) => g,
        Err(_) => return true,
    };

    let Some(ref allowed) = *list else {
        return true;
    };

    client_ip.parse::<IpAddr>().map_or(false, |ip| allowed.contains(&ip))
}

#[pyfunction]
fn set_ip_allowlist(ips: Vec<String>) -> PyResult<()> {
    let parsed: std::collections::HashSet<IpAddr> = ips
        .iter()
        .filter_map(|s| s.parse::<IpAddr>().ok())
        .collect();

    match IP_ALLOWLIST.write() {
        Ok(mut guard) => {
            *guard = Some(parsed);
            Ok(())
        }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("IP allowlist lock poisoned")),
    }
}

#[pyfunction]
fn clear_ip_allowlist() -> PyResult<()> {
    match IP_ALLOWLIST.write() {
        Ok(mut guard) => {
            *guard = None;
            Ok(())
        }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("IP allowlist lock poisoned")),
    }
}

#[pyfunction]
fn set_policy_callback(callback: Py<PyAny>) -> PyResult<()> {
    match POLICY_CALLBACK.write() {
        Ok(mut guard) => {
            *guard = Some(callback);
            Ok(())
        }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("Policy callback lock poisoned")),
    }
}

#[pyfunction]
fn clear_policy_callback() -> PyResult<()> {
    match POLICY_CALLBACK.write() {
        Ok(mut guard) => {
            *guard = None;
            Ok(())
        }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("Policy callback lock poisoned")),
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
        .to_string()
}

// Returns None = allowed, Some(reason) = denied.
// Calls the Python policy callback synchronously — must run on spawn_blocking.
fn check_policy_sync(api_key: String, tool_name: String) -> Option<String> {
    Python::attach(|py| -> Option<String> {
        let guard = POLICY_CALLBACK.read().ok()?;
        let callback = guard.as_ref()?;
        let result = callback.call1(py, (&api_key, &tool_name)).ok()?;
        let tuple = result.bind(py);
        let allowed: bool = tuple.get_item(0).ok()?.extract().ok()?;
        if allowed {
            None
        } else {
            let reason: Option<String> = tuple.get_item(1).ok()?.extract().ok().flatten();
            Some(reason.unwrap_or_else(|| "Forbidden by policy".to_string()))
        }
    })
}

#[pyfunction]
fn set_tool_filter_callback(callback: Py<PyAny>) -> PyResult<()> {
    match TOOL_FILTER_CALLBACK.write() {
        Ok(mut guard) => { *guard = Some(callback); Ok(()) }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("Tool filter callback lock poisoned")),
    }
}

#[pyfunction]
fn clear_tool_filter_callback() -> PyResult<()> {
    match TOOL_FILTER_CALLBACK.write() {
        Ok(mut guard) => { *guard = None; Ok(()) }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("Tool filter callback lock poisoned")),
    }
}

// Returns None = show all tools, Some(patterns) = restrict to matching tools.
// Must run on spawn_blocking.
fn get_tenant_tool_filter(api_key: String) -> Option<Vec<String>> {
    Python::attach(|py| -> Option<Vec<String>> {
        let guard = TOOL_FILTER_CALLBACK.read().ok()?;
        let callback = guard.as_ref()?;
        let result = callback.call1(py, (&api_key,)).ok()?;
        let bound = result.bind(py);
        if bound.is_none() {
            return None;
        }
        let patterns: Vec<String> = bound.extract().ok()?;
        // Wildcard means "all tools" — same as no filter.
        if patterns.iter().any(|p| p == "*") {
            return None;
        }
        Some(patterns)
    })
}

// Checks whether a tool name matches any pattern in the filter list.
// Supported patterns: exact ("add"), namespace wildcard ("github.*"), global ("*").
fn tool_name_matches_filter(name: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|p| {
        if p == "*" {
            return true;
        }
        if let Some(prefix) = p.strip_suffix(".*") {
            return name.starts_with(&format!("{prefix}.")) || name == prefix;
        }
        name == p.as_str()
    })
}

fn generate_hex_id(bytes: usize) -> String {
    use rand::Rng as _;
    let mut rng = rand::rng();
    (0..bytes).map(|_| format!("{:02x}", rng.random::<u8>())).collect()
}

fn parse_traceparent(header: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = header.splitn(4, '-').collect();
    if parts.len() != 4 { return None; }
    let trace_id = parts[1];
    let parent_id = parts[2];
    if trace_id.len() == 32 && parent_id.len() == 16 {
        Some((trace_id.to_string(), parent_id.to_string()))
    } else {
        None
    }
}

async fn export_otel_span(
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    operation: String,
    start_ns: u64,
    end_ns: u64,
    status_code: u16,
) {
    if !OTEL_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let endpoint = {
        let guard = match OTEL_ENDPOINT.read() {
            Ok(g) => g,
            Err(_) => return,
        };
        match guard.as_deref() {
            Some(ep) => ep.to_string(),
            None => return,
        }
    };
    let service_name = OTEL_SERVICE_NAME.read()
        .map(|g| g.clone())
        .unwrap_or_else(|_| "kurd".to_string());

    let otel_status = if status_code < 400 { 1i64 } else { 2i64 };
    let mut span = serde_json::json!({
        "traceId": trace_id,
        "spanId": span_id,
        "name": operation,
        "kind": 2,
        "startTimeUnixNano": start_ns.to_string(),
        "endTimeUnixNano": end_ns.to_string(),
        "attributes": [
            {"key": "http.status_code", "value": {"intValue": status_code as i64}}
        ],
        "status": {"code": otel_status}
    });
    if let Some(pid) = parent_span_id {
        if !pid.is_empty() {
            span["parentSpanId"] = serde_json::json!(pid);
        }
    }
    let payload = serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": service_name}},
                    {"key": "telemetry.sdk.name", "value": {"stringValue": "kurd-rust"}}
                ]
            },
            "scopeSpans": [{
                "scope": {"name": "kurd"},
                "spans": [span]
            }]
        }]
    });
    let url = format!("{}/v1/traces", endpoint);
    let _ = HTTP_CLIENT
        .post(&url)
        .header("content-type", "application/json")
        .body(payload.to_string())
        .timeout(Duration::from_millis(2000))
        .send()
        .await;
}

#[pyfunction]
fn configure_otel(endpoint: String, service_name: String) -> PyResult<()> {
    match OTEL_ENDPOINT.write() {
        Ok(mut g) => { *g = Some(endpoint); }
        Err(_) => return Err(pyo3::exceptions::PyRuntimeError::new_err("OTEL lock poisoned")),
    }
    match OTEL_SERVICE_NAME.write() {
        Ok(mut g) => { *g = service_name; }
        Err(_) => return Err(pyo3::exceptions::PyRuntimeError::new_err("OTEL lock poisoned")),
    }
    OTEL_ENABLED.store(true, Ordering::Relaxed);
    Ok(())
}

#[pyfunction]
fn clear_otel() -> PyResult<()> {
    OTEL_ENABLED.store(false, Ordering::Relaxed);
    if let Ok(mut g) = OTEL_ENDPOINT.write() {
        *g = None;
    }
    Ok(())
}

const OVERLOAD_ERROR_CODE: i64 = -32029;
const RATE_LIMIT_ERROR_CODE: i64 = -32028;
const POLICY_DENIED_ERROR_CODE: i64 = -32004;

static POLICY_DENIED_REQUESTS: AtomicU64 = AtomicU64::new(0);

static POLICY_CALLBACK: Lazy<RwLock<Option<Py<PyAny>>>> =
    Lazy::new(|| RwLock::new(None));

// Called on tools/list: (api_key: str) -> Optional[List[str]]
// None = show all tools; a list = show only tools matching those patterns.
static TOOL_FILTER_CALLBACK: Lazy<RwLock<Option<Py<PyAny>>>> =
    Lazy::new(|| RwLock::new(None));

static OTEL_ENABLED: AtomicBool = AtomicBool::new(false);
static OTEL_ENDPOINT: Lazy<RwLock<Option<String>>> = Lazy::new(|| RwLock::new(None));
static OTEL_SERVICE_NAME: Lazy<RwLock<String>> = Lazy::new(|| RwLock::new("kurd".to_string()));

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

fn check_rate_limit(client_ip: &str) -> bool {
    if !RATE_LIMIT_ENABLED.load(Ordering::Relaxed) {
        return true;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let window_start = now.saturating_sub(1000);

    if let Ok(mut requests) = RATE_LIMIT_REQUESTS.lock() {
        // Sweep stale IP entries at most once per minute to prevent unbounded memory growth.
        let last_cleanup = RATE_LIMIT_LAST_CLEANUP_MS.load(Ordering::Relaxed);
        if now.saturating_sub(last_cleanup) > 60_000 {
            requests.retain(|_, timestamps| timestamps.iter().any(|&t| t > window_start));
            RATE_LIMIT_LAST_CLEANUP_MS.store(now, Ordering::Relaxed);
        }

        let entry = requests.entry(client_ip.to_string()).or_insert_with(Vec::new);

        entry.retain(|&timestamp| timestamp > window_start);

        let per_ip_limit = RATE_LIMIT_PER_IP_RPS.load(Ordering::Relaxed);
        if entry.len() >= per_ip_limit as usize {
            return false;
        }

        entry.push(now);

        let global_limit = RATE_LIMIT_GLOBAL_RPS.load(Ordering::Relaxed);
        let total_requests: usize = requests.values().map(|v| v.len()).sum();

        if total_requests > global_limit as usize {
            return false;
        }
    }

    true
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

    tracing::info!(
        event = "kurd.http.request",
        request_id = request_id,
        method = method,
        status = status.as_u16(),
        latency_ms = latency_ms,
        body_bytes = body_bytes,
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
    // true while a single probe request is in-flight after the reset timeout.
    half_open: bool,
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

static TOOLS_CACHE_TTL_MS: AtomicU64 = AtomicU64::new(30_000);

fn tools_cache_ttl() -> Duration {
    Duration::from_millis(TOOLS_CACHE_TTL_MS.load(Ordering::Relaxed))
}

const MCP_PROTOCOL_VERSION: &str = "2026-07-28";
const MCP_HEADER_MISMATCH: i64 = -32020;
const MCP_UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;
const MCP_LIST_TTL_MS: u64 = 30_000;
const MCP_CACHE_SCOPE: &str = "public";
const TOOLS_LIST_PAGE_SIZE: usize = 100;


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
            // Names present and matching — OK.
            (Some(h), Some(b)) if h == b => {}
            // Names present but mismatched — header error.
            (Some(h), Some(b)) => {
                return Err(jsonrpc_error(
                    StatusCode::BAD_REQUEST,
                    request_id.clone(),
                    MCP_HEADER_MISMATCH,
                    "Mcp-Name header does not match tool name",
                    Some(serde_json::json!({ "header": h, "body": b })),
                ));
            }
            // No header on a modern request — header error.
            (None, Some(_)) => {
                return Err(jsonrpc_error(
                    StatusCode::BAD_REQUEST,
                    request_id.clone(),
                    MCP_HEADER_MISMATCH,
                    "Missing Mcp-Name header",
                    None,
                ));
            }
            // body_name is None — let param validation below return -32602.
            _ => {}
        }
    }

    Ok(())
}


async fn list_upstream_tools() -> Vec<Value> {
    if let Ok(cache) = TOOLS_CACHE.read() {
        if let Some(cache) = cache.as_ref() {
            if cache.created_at.elapsed() < tools_cache_ttl() {
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
            let mut all_tools: Vec<Value> = Vec::new();
            let mut cursor: Option<String> = None;

            loop {
                let mut params = serde_json::json!({
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL_VERSION,
                        "io.modelcontextprotocol/clientInfo": {
                            "name": "kurd",
                            "version": env!("CARGO_PKG_VERSION")
                        },
                        "io.modelcontextprotocol/clientCapabilities": {}
                    }
                });

                if let Some(ref c) = cursor {
                    params["cursor"] = Value::String(c.clone());
                }

                let payload = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": format!("kurd-tools-list-{upstream_name}"),
                    "method": "tools/list",
                    "params": params
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
                    Ok(r) if r.status().is_success() => r,
                    _ => break,
                };

                let body: Value = match response.json().await {
                    Ok(v) => v,
                    Err(_) => break,
                };

                let tools = match body
                    .get("result")
                    .and_then(|r| r.get("tools"))
                    .and_then(|t| t.as_array())
                {
                    Some(t) => t.clone(),
                    None => break,
                };

                for tool in &tools {
                    let remote_name = match tool
                        .get("name")
                        .and_then(|v| v.as_str())
                    {
                        Some(n) => n,
                        None => continue,
                    };

                    let mut tool = tool.clone();
                    if let Some(obj) = tool.as_object_mut() {
                        obj.insert(
                            "name".to_string(),
                            Value::String(format!("{upstream_name}.{remote_name}")),
                        );
                    }
                    all_tools.push(tool);
                }

                cursor = body
                    .get("result")
                    .and_then(|r| r.get("nextCursor"))
                    .and_then(|c| c.as_str())
                    .map(str::to_owned);

                if cursor.is_none() {
                    break;
                }
            }

            all_tools
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
                half_open: false,
            });

        if let Some(opened_at) = state.opened_at {
            if opened_at.elapsed() < CIRCUIT_RESET_TIMEOUT {
                // Circuit is fully open: reject all requests.
                if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
                    metrics
                        .entry(upstream_name.to_string())
                        .or_default()
                        .failures += 1;
                }
                return Err(format!("Circuit open for upstream: {upstream_name}"));
            }

            // Reset timeout elapsed — allow exactly one probe request.
            if state.half_open {
                // Probe already in flight: reject this request.
                if let Ok(mut metrics) = UPSTREAM_METRICS.write() {
                    metrics
                        .entry(upstream_name.to_string())
                        .or_default()
                        .failures += 1;
                }
                return Err(format!("Circuit half-open for upstream: {upstream_name}"));
            }

            // Mark probe in-flight; do not reset failure count until probe succeeds.
            state.half_open = true;
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

        // Propagate W3C trace context: create a child span for this upstream hop.
        if let Ok(parent_tp) = TASK_TRACEPARENT.try_with(|t| t.clone()) {
            let parts: Vec<&str> = parent_tp.split('-').collect();
            if parts.len() >= 4 {
                let child_tp = format!("00-{}-{}-01", parts[1], generate_hex_id(8));
                request = request.header("traceparent", child_tp);
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
                    // Enforce response size limit before buffering.
                    if let Some(cl) = response.content_length() {
                        if cl > MAX_UPSTREAM_RESPONSE_BYTES as u64 {
                            return Err(format!(
                                "Upstream response too large: {cl} bytes (max {MAX_UPSTREAM_RESPONSE_BYTES})"
                            ));
                        }
                    }
                    let bytes = response.bytes().await.map_err(|e| e.to_string())?;
                    if bytes.len() > MAX_UPSTREAM_RESPONSE_BYTES {
                        return Err(format!(
                            "Upstream response too large: {} bytes (max {MAX_UPSTREAM_RESPONSE_BYTES})",
                            bytes.len()
                        ));
                    }
                    let value = serde_json::from_slice::<Value>(&bytes)
                        .map_err(|e| e.to_string())?;

                    // Probe or normal success — fully close the circuit.
                    if let Ok(mut breakers) = CIRCUIT_BREAKERS.write() {
                        breakers.insert(
                            upstream_name.to_string(),
                            CircuitState {
                                failures: 0,
                                opened_at: None,
                                half_open: false,
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
                half_open: false,
            });

        state.failures += 1;

        // Half-open probe failure: re-open immediately without waiting for the threshold.
        if state.half_open || state.failures >= CIRCUIT_FAILURE_THRESHOLD {
            state.opened_at = Some(Instant::now());
            state.half_open = false;
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
        notify_tools_changed();
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
fn set_rate_limiting(enabled: bool, per_ip_rps: u64, global_rps: u64) -> PyResult<()> {
    RATE_LIMIT_ENABLED.store(enabled, Ordering::Release);
    RATE_LIMIT_PER_IP_RPS.store(per_ip_rps, Ordering::Release);
    RATE_LIMIT_GLOBAL_RPS.store(global_rps, Ordering::Release);
    Ok(())
}

#[pyfunction]
fn runtime_status(py: Python<'_>) -> PyResult<pyo3::Py<pyo3::types::PyDict>> {
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("globalConcurrencyLimit", GLOBAL_CONCURRENCY_LIMIT.load(Ordering::Acquire))?;
    dict.set_item("upstreamConcurrencyLimit", UPSTREAM_CONCURRENCY_LIMIT.load(Ordering::Acquire))?;
    dict.set_item("pythonConcurrencyLimit", PYTHON_CONCURRENCY_LIMIT.load(Ordering::Acquire))?;
    dict.set_item("activeRequests", GLOBAL_ACTIVE_REQUESTS.load(Ordering::Acquire))?;
    dict.set_item("peakActiveRequests", GLOBAL_PEAK_ACTIVE_REQUESTS.load(Ordering::Acquire))?;
    dict.set_item("totalRequests", TOTAL_HTTP_REQUESTS.load(Ordering::Acquire))?;
    dict.set_item("completedRequests", COMPLETED_HTTP_REQUESTS.load(Ordering::Acquire))?;
    dict.set_item("rejectedRequests", REJECTED_HTTP_REQUESTS.load(Ordering::Acquire))?;
    dict.set_item("pythonRejectedCalls", PYTHON_REJECTIONS.load(Ordering::Acquire))?;
    dict.set_item("requestLoggingEnabled", REQUEST_LOGGING_ENABLED.load(Ordering::Acquire))?;
    Ok(dict.unbind())
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
fn set_admin_token(token: String) -> PyResult<()> {
    match ADMIN_TOKEN.write() {
        Ok(mut guard) => { *guard = Some(token); Ok(()) }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("Admin token lock poisoned")),
    }
}

#[pyfunction]
fn clear_admin_token() -> PyResult<()> {
    match ADMIN_TOKEN.write() {
        Ok(mut guard) => { *guard = None; Ok(()) }
        Err(_) => Err(pyo3::exceptions::PyRuntimeError::new_err("Admin token lock poisoned")),
    }
}

// Returns true if the request carries a valid admin credential.
// Priority: dedicated admin token → fallback to bearer token → open if neither configured.
fn authorize_admin_request(headers: &HeaderMap) -> bool {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    if let Ok(guard) = ADMIN_TOKEN.read() {
        if let Some(ref expected) = *guard {
            return constant_time_eq(provided, expected);
        }
    }

    // No dedicated admin token — fall back to the regular bearer token.
    if let Ok(guard) = HTTP_BEARER_TOKEN.read() {
        if let Some(ref expected) = *guard {
            return constant_time_eq(provided, expected);
        }
    }

    true // neither token configured → dev/open mode
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
fn set_tools_cache_ttl_ms(ttl_ms: u64) -> PyResult<()> {
    if ttl_ms == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Tools cache TTL must be greater than zero",
        ));
    }

    TOOLS_CACHE_TTL_MS.store(ttl_ms, Ordering::Relaxed);
    invalidate_tools_cache();
    Ok(())
}

#[pyfunction]
fn list_upstreams() -> PyResult<Vec<(String, String)>> {
    let registry = UPSTREAM_REGISTRY
        .read()
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err(
            "Upstream registry lock poisoned"
        ))?;

    Ok(registry.iter().map(|(n, u)| (n.clone(), u.clone())).collect())
}

#[pyfunction]
fn list_tools() -> PyResult<Vec<String>> {
    let registry = TOOL_REGISTRY
        .read()
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err(
            "Tool registry lock poisoned"
        ))?;

    Ok(registry.keys().cloned().collect())
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
    notify_tools_changed();

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
fn unregister_tool(name: String) -> PyResult<bool> {
    let mut tools = TOOL_REGISTRY
        .write()
        .map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Tool registry lock poisoned"
            )
        })?;

    let removed = tools.remove(&name).is_some();
    drop(tools);

    if removed {
        invalidate_tools_cache();
        notify_tools_changed();
    }

    Ok(removed)
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
    drop(tools);
    notify_tools_changed();

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

async fn mcp_options() -> impl IntoResponse {
    (
        StatusCode::NO_CONTENT,
        [
            ("access-control-allow-origin", "*"),
            ("access-control-allow-methods", "POST, OPTIONS"),
            ("access-control-allow-headers",
             "content-type, authorization, mcp-protocol-version, mcp-method, mcp-session-id"),
            ("access-control-max-age", "86400"),
        ],
        "",
    )
}

// ---------------------------------------------------------
// Admin API handlers
// ---------------------------------------------------------

#[derive(Deserialize)]
struct AddServerRequest {
    name: String,
    url: String,
}

async fn admin_list_servers(headers: HeaderMap) -> impl IntoResponse {
    if !authorize_admin_request(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let upstreams: Vec<(String, String)> = UPSTREAM_REGISTRY
        .read()
        .map(|r| r.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    let metrics = UPSTREAM_METRICS.read().ok();
    let breakers = CIRCUIT_BREAKERS.read().ok();

    let servers: Vec<Value> = upstreams.iter().map(|(name, url)| {
        let m = metrics.as_ref().and_then(|m| m.get(name));
        let b = breakers.as_ref().and_then(|b| b.get(name));

        let circuit_state = match b {
            Some(s) if s.opened_at.map_or(false, |t| t.elapsed() < CIRCUIT_RESET_TIMEOUT) => "open",
            _ => "closed",
        };

        serde_json::json!({
            "name": name,
            "url": url,
            "circuitBreaker": circuit_state,
            "metrics": {
                "requests": m.map_or(0, |x| x.requests),
                "successes": m.map_or(0, |x| x.successes),
                "failures": m.map_or(0, |x| x.failures),
                "retries": m.map_or(0, |x| x.retries),
                "avgLatencyMs": m.map_or(0.0, |x| {
                    if x.successes > 0 { x.total_latency_ms as f64 / x.successes as f64 } else { 0.0 }
                }),
                "lastLatencyMs": m.map_or(0, |x| x.last_latency_ms)
            }
        })
    }).collect();

    (StatusCode::OK, Json(serde_json::json!({"servers": servers, "count": servers.len()}))).into_response()
}

async fn admin_add_server(headers: HeaderMap, Json(body): Json<AddServerRequest>) -> impl IntoResponse {
    if !authorize_admin_request(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    if body.name.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "name is required"}))).into_response();
    }

    if let Err(e) = validate_upstream_url(&body.url) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": e}))).into_response();
    }

    match UPSTREAM_REGISTRY.write() {
        Ok(mut registry) => {
            let replaced = registry.contains_key(&body.name);
            registry.insert(body.name.clone(), body.url.clone());
            if let Ok(mut cache) = TOOLS_CACHE.write() { *cache = None; }
            if let Ok(mut metrics) = TOOLS_CACHE_METRICS.write() { metrics.invalidations += 1; }
            notify_tools_changed();
            let status = if replaced { StatusCode::OK } else { StatusCode::CREATED };
            (status, Json(serde_json::json!({"name": body.name, "url": body.url, "replaced": replaced}))).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Registry lock poisoned"}))).into_response(),
    }
}

async fn admin_delete_server(headers: HeaderMap, Path(name): Path<String>) -> impl IntoResponse {
    if !authorize_admin_request(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    match UPSTREAM_REGISTRY.write() {
        Ok(mut registry) => {
            if registry.remove(&name).is_some() {
                if let Ok(mut cache) = TOOLS_CACHE.write() { *cache = None; }
                if let Ok(mut metrics) = TOOLS_CACHE_METRICS.write() { metrics.invalidations += 1; }
                notify_tools_changed();
                (StatusCode::OK, Json(serde_json::json!({"deleted": name}))).into_response()
            } else {
                (StatusCode::NOT_FOUND, Json(serde_json::json!({"error": format!("Server not found: {name}")}))).into_response()
            }
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "Registry lock poisoned"}))).into_response(),
    }
}

async fn admin_list_tools(headers: HeaderMap) -> impl IntoResponse {
    if !authorize_admin_request(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let upstream_tools = list_upstream_tools().await;

    let local_tools: Vec<Value> = TOOL_REGISTRY.read().map(|r| {
        r.iter().map(|(name, t)| serde_json::json!({
            "name": name,
            "description": t.description,
            "source": "local",
            "inputSchema": t.input_schema
        })).collect()
    }).unwrap_or_default();

    let upstream_annotated: Vec<Value> = upstream_tools.into_iter().map(|mut t| {
        let source = t.get("name")
            .and_then(|n| n.as_str())
            .and_then(|n| n.split_once('.'))
            .map(|(upstream, _)| upstream.to_string())
            .unwrap_or_else(|| "upstream".to_string());
        if let Some(obj) = t.as_object_mut() {
            obj.insert("source".to_string(), Value::String(source));
        }
        t
    }).collect();

    let local_count = local_tools.len();
    let upstream_count = upstream_annotated.len();
    let mut all_tools = local_tools;
    all_tools.extend(upstream_annotated);
    let total = all_tools.len();
    (StatusCode::OK, Json(serde_json::json!({
        "tools": all_tools,
        "count": total,
        "localCount": local_count,
        "upstreamCount": upstream_count
    }))).into_response()
}

async fn admin_reload_tools(headers: HeaderMap) -> impl IntoResponse {
    if !authorize_admin_request(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    if let Ok(mut cache) = TOOLS_CACHE.write() { *cache = None; }
    if let Ok(mut metrics) = TOOLS_CACHE_METRICS.write() { metrics.invalidations += 1; }
    notify_tools_changed();

    (StatusCode::OK, Json(serde_json::json!({"reloaded": true}))).into_response()
}

async fn admin_list_namespaces(headers: HeaderMap) -> impl IntoResponse {
    if !authorize_admin_request(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Unauthorized"}))).into_response();
    }

    let upstream_namespaces: Vec<Value> = UPSTREAM_REGISTRY
        .read()
        .map(|r| r.keys().map(|k| serde_json::json!({"namespace": k, "source": "upstream"})).collect())
        .unwrap_or_default();

    let has_local = TOOL_REGISTRY
        .read()
        .map(|r| !r.is_empty())
        .unwrap_or(false);

    let mut namespaces = upstream_namespaces;
    if has_local {
        namespaces.push(serde_json::json!({"namespace": "local", "source": "local"}));
    }

    (StatusCode::OK, Json(serde_json::json!({"namespaces": namespaces, "count": namespaces.len()}))).into_response()
}

/// GET /mcp — Streamable HTTP SSE endpoint per MCP 2026-07-28 spec.
/// Clients subscribe here to receive server-initiated notifications
/// (e.g. notifications/tools/list_changed).
async fn mcp_sse(headers: HeaderMap) -> impl IntoResponse {
    let session_id = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(|| generate_hex_id(16));

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    if let Ok(mut channels) = SSE_CHANNELS.lock() {
        // Remove stale (closed) channels when we grow large.
        if channels.len() > 500 {
            channels.retain(|_, sender| !sender.is_closed());
        }
        channels.insert(session_id, tx);
    }

    let stream = UnboundedReceiverStream::new(rx)
        .map(|data| Ok::<Event, Infallible>(Event::default().data(data)));

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Broadcast a notifications/tools/list_changed notification to all SSE subscribers.
fn notify_tools_changed() {
    let channels = match SSE_CHANNELS.lock() {
        Ok(c) => c,
        Err(_) => return,
    };
    if channels.is_empty() {
        return;
    }
    let notification = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/tools/list_changed",
        "params": {}
    })
    .to_string();
    for tx in channels.values() {
        let _ = tx.send(notification.clone());
    }
}

fn build_http_router() -> AxumRouter {
    AxumRouter::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/metrics", get(metrics_prometheus))
        .route("/mcp", get(mcp_sse).post(mcp_root).options(mcp_options))
        .route("/admin/servers", get(admin_list_servers).post(admin_add_server))
        .route("/admin/servers/{name}", delete(admin_delete_server))
        .route("/admin/tools", get(admin_list_tools))
        .route("/admin/tools/reload", post(admin_reload_tools))
        .route("/admin/tools/namespaces", get(admin_list_namespaces))
        .layer(DefaultBodyLimit::max(MAX_MCP_BODY_BYTES))
}

async fn run_http_server(
    addr: &str,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let app = build_http_router();

    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
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
        // Install a default subscriber if the host hasn't set one up yet.
        // Respects RUST_LOG / KURD_LOG env vars; falls back to info-level.
        // Uses a reload::Layer so logging/setLevel can change the filter at runtime.
        let initial_filter = tracing_subscriber::EnvFilter::try_from_env("KURD_LOG")
            .or_else(|_| tracing_subscriber::EnvFilter::try_from_default_env())
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("kurd=info"));
        let (filter_layer, reload_handle) =
            tracing_subscriber::reload::Layer::new(initial_filter);
        let init_result = tracing_subscriber::registry()
            .with(filter_layer)
            .with(tracing_subscriber::fmt::layer())
            .try_init();
        // On subsequent gateway starts the global is already set; that is fine.
        let _ = init_result;
        if let Ok(mut h) = LOG_FILTER_HANDLE.lock() {
            *h = Some(reload_handle);
        }

        // Auto-load bearer token from environment if not already configured.
        if let Ok(token) = std::env::var("KURD_AUTH_TOKEN") {
            if !token.is_empty() {
                if let Ok(mut guard) = HTTP_BEARER_TOKEN.write() {
                    if guard.is_none() {
                        *guard = Some(token);
                    }
                }
            }
        }

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
    payloads
        .into_par_iter()
        .map(|payload| {
            let parsed: Value = serde_json::from_str(&payload)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

            let method = parsed.get("method").and_then(|v| v.as_str()).map(str::to_owned);
            let id = parsed.get("id").map(|v| v.to_string());
            let params = parsed.get("params").map(|v| v.to_string());

            Ok((method, id, params))
        })
        .collect()
}

#[pymodule]
fn _kurd(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fast_parse, m)?)?;
    m.add_function(wrap_pyfunction!(fast_parse_batch, m)?)?;
    m.add_function(wrap_pyfunction!(start_http_gateway, m)?)?;
    m.add_function(wrap_pyfunction!(stop_http_gateway, m)?)?;
    m.add_function(wrap_pyfunction!(http_gateway_status, m)?)?;
    m.add_function(wrap_pyfunction!(register_tool, m)?)?;
    m.add_function(wrap_pyfunction!(unregister_tool, m)?)?;
    m.add_function(wrap_pyfunction!(init_python_async_runtime, m)?)?;
    m.add_function(wrap_pyfunction!(register_upstream, m)?)?;
    m.add_function(wrap_pyfunction!(unregister_upstream, m)?)?;
    m.add_function(wrap_pyfunction!(clear_tools_cache, m)?)?;
    m.add_function(wrap_pyfunction!(set_http_bearer_token, m)?)?;
    m.add_function(wrap_pyfunction!(clear_http_bearer_token, m)?)?;
    m.add_function(wrap_pyfunction!(set_allow_private_upstreams, m)?)?;
    m.add_function(wrap_pyfunction!(set_upstream_timeout_ms, m)?)?;
    m.add_function(wrap_pyfunction!(set_tools_cache_ttl_ms, m)?)?;
    m.add_function(wrap_pyfunction!(list_upstreams, m)?)?;
    m.add_function(wrap_pyfunction!(list_tools, m)?)?;
    m.add_function(wrap_pyfunction!(security_status, m)?)?;
    m.add_function(wrap_pyfunction!(set_runtime_limits, m)?)?;
    m.add_function(wrap_pyfunction!(set_request_logging, m)?)?;
    m.add_function(wrap_pyfunction!(set_rate_limiting, m)?)?;
    m.add_function(wrap_pyfunction!(runtime_status, m)?)?;
    m.add_function(wrap_pyfunction!(set_ip_allowlist, m)?)?;
    m.add_function(wrap_pyfunction!(clear_ip_allowlist, m)?)?;
    m.add_function(wrap_pyfunction!(set_policy_callback, m)?)?;
    m.add_function(wrap_pyfunction!(clear_policy_callback, m)?)?;
    m.add_function(wrap_pyfunction!(set_tool_filter_callback, m)?)?;
    m.add_function(wrap_pyfunction!(clear_tool_filter_callback, m)?)?;
    m.add_function(wrap_pyfunction!(set_admin_token, m)?)?;
    m.add_function(wrap_pyfunction!(clear_admin_token, m)?)?;
    m.add_function(wrap_pyfunction!(configure_otel, m)?)?;
    m.add_function(wrap_pyfunction!(clear_otel, m)?)?;

    Ok(())
}


async fn health() -> &'static str {
    "Kurd MCP Gateway"
}

async fn metrics_prometheus() -> impl IntoResponse {
    let mut output = String::new();

    // Help text
    output.push_str("# HELP kurd_requests_total Total HTTP requests received\n");
    output.push_str("# TYPE kurd_requests_total counter\n");

    // Runtime metrics
    let total_requests = TOTAL_HTTP_REQUESTS.load(Ordering::Acquire);
    let completed_requests = COMPLETED_HTTP_REQUESTS.load(Ordering::Acquire);
    let rejected_requests = REJECTED_HTTP_REQUESTS.load(Ordering::Acquire);
    let active_requests = GLOBAL_ACTIVE_REQUESTS.load(Ordering::Acquire);
    let peak_active_requests = GLOBAL_PEAK_ACTIVE_REQUESTS.load(Ordering::Acquire);

    let policy_denied = POLICY_DENIED_REQUESTS.load(Ordering::Acquire);

    output.push_str(&format!("kurd_requests_total {{status=\"total\"}} {}\n", total_requests));
    output.push_str(&format!("kurd_requests_total {{status=\"completed\"}} {}\n", completed_requests));
    output.push_str(&format!("kurd_requests_total {{status=\"rejected\"}} {}\n", rejected_requests));

    output.push_str("# HELP kurd_policy_denied_total Requests denied by the policy engine\n");
    output.push_str("# TYPE kurd_policy_denied_total counter\n");
    output.push_str(&format!("kurd_policy_denied_total {}\n", policy_denied));

    output.push_str("# HELP kurd_requests_active Active HTTP requests\n");
    output.push_str("# TYPE kurd_requests_active gauge\n");
    output.push_str(&format!("kurd_requests_active {}\n", active_requests));

    output.push_str("# HELP kurd_requests_peak_active Peak active HTTP requests\n");
    output.push_str("# TYPE kurd_requests_peak_active gauge\n");
    output.push_str(&format!("kurd_requests_peak_active {}\n", peak_active_requests));

    // Latency metrics
    let total_latency_ms = TOTAL_HTTP_LATENCY_MS.load(Ordering::Acquire);
    let avg_latency_ms = if completed_requests > 0 {
        total_latency_ms as f64 / completed_requests as f64
    } else {
        0.0
    };

    output.push_str("# HELP kurd_request_latency_ms Average request latency in milliseconds\n");
    output.push_str("# TYPE kurd_request_latency_ms gauge\n");
    output.push_str(&format!("kurd_request_latency_ms {:.2}\n", avg_latency_ms));

    // Python callback metrics
    let python_active = PYTHON_ACTIVE_CALLS.load(Ordering::Acquire);
    let python_peak = PYTHON_PEAK_ACTIVE_CALLS.load(Ordering::Acquire);
    let python_rejections = PYTHON_REJECTIONS.load(Ordering::Acquire);

    output.push_str("# HELP kurd_python_active_calls Active Python tool calls\n");
    output.push_str("# TYPE kurd_python_active_calls gauge\n");
    output.push_str(&format!("kurd_python_active_calls {}\n", python_active));

    output.push_str("# HELP kurd_python_peak_active_calls Peak active Python tool calls\n");
    output.push_str("# TYPE kurd_python_peak_active_calls gauge\n");
    output.push_str(&format!("kurd_python_peak_active_calls {}\n", python_peak));

    output.push_str("# HELP kurd_python_rejections_total Python tool call rejections\n");
    output.push_str("# TYPE kurd_python_rejections_total counter\n");
    output.push_str(&format!("kurd_python_rejections_total {}\n", python_rejections));

    // Concurrency limits
    output.push_str("# HELP kurd_concurrency_limit Concurrency limits\n");
    output.push_str("# TYPE kurd_concurrency_limit gauge\n");
    let global_limit = GLOBAL_CONCURRENCY_LIMIT.load(Ordering::Acquire);
    let upstream_limit = UPSTREAM_CONCURRENCY_LIMIT.load(Ordering::Acquire);
    let python_limit = PYTHON_CONCURRENCY_LIMIT.load(Ordering::Acquire);

    output.push_str(&format!("kurd_concurrency_limit {{type=\"global\"}} {}\n", global_limit));
    output.push_str(&format!("kurd_concurrency_limit {{type=\"upstream\"}} {}\n", upstream_limit));
    output.push_str(&format!("kurd_concurrency_limit {{type=\"python\"}} {}\n", python_limit));

    // Upstream metrics
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

    if !upstreams.is_empty() {
        output.push_str("# HELP kurd_upstream_requests_total Upstream MCP server requests\n");
        output.push_str("# TYPE kurd_upstream_requests_total counter\n");

        for (name, _url) in &upstreams {
            let metric = metrics.get(name).cloned().unwrap_or_default();
            output.push_str(&format!("kurd_upstream_requests_total {{upstream=\"{}\"}} {}\n", name, metric.requests));
        }

        output.push_str("# HELP kurd_upstream_successes_total Successful upstream calls\n");
        output.push_str("# TYPE kurd_upstream_successes_total counter\n");

        for (name, _url) in &upstreams {
            let metric = metrics.get(name).cloned().unwrap_or_default();
            output.push_str(&format!("kurd_upstream_successes_total {{upstream=\"{}\"}} {}\n", name, metric.successes));
        }

        output.push_str("# HELP kurd_upstream_failures_total Failed upstream calls\n");
        output.push_str("# TYPE kurd_upstream_failures_total counter\n");

        for (name, _url) in &upstreams {
            let metric = metrics.get(name).cloned().unwrap_or_default();
            output.push_str(&format!("kurd_upstream_failures_total {{upstream=\"{}\"}} {}\n", name, metric.failures));
        }

        output.push_str("# HELP kurd_upstream_retries_total Upstream call retries\n");
        output.push_str("# TYPE kurd_upstream_retries_total counter\n");

        for (name, _url) in &upstreams {
            let metric = metrics.get(name).cloned().unwrap_or_default();
            output.push_str(&format!("kurd_upstream_retries_total {{upstream=\"{}\"}} {}\n", name, metric.retries));
        }

        output.push_str("# HELP kurd_upstream_latency_ms Average upstream call latency\n");
        output.push_str("# TYPE kurd_upstream_latency_ms gauge\n");

        for (name, _url) in &upstreams {
            let metric = metrics.get(name).cloned().unwrap_or_default();
            let avg_latency = if metric.successes + metric.failures > 0 {
                metric.total_latency_ms as f64 / (metric.successes + metric.failures) as f64
            } else {
                0.0
            };
            output.push_str(&format!("kurd_upstream_latency_ms {{upstream=\"{}\"}} {:.2}\n", name, avg_latency));
        }

        output.push_str("# HELP kurd_upstream_circuit_breaker_state Circuit breaker state (0=closed, 1=open)\n");
        output.push_str("# TYPE kurd_upstream_circuit_breaker_state gauge\n");

        for (name, _url) in &upstreams {
            let breaker = breakers.get(name).copied().unwrap_or(CircuitState {
                failures: 0,
                opened_at: None,
                half_open: false,
            });
            let state = match breaker.opened_at {
                Some(opened_at) if opened_at.elapsed() < CIRCUIT_RESET_TIMEOUT => 1,
                _ => 0,
            };
            output.push_str(&format!("kurd_upstream_circuit_breaker_state {{upstream=\"{}\"}} {}\n", name, state));
        }
    }

    // Cache metrics
    let cache_metrics = TOOLS_CACHE_METRICS
        .read()
        .map(|metrics| *metrics)
        .unwrap_or_default();

    output.push_str("# HELP kurd_cache_hits_total Tool cache hits\n");
    output.push_str("# TYPE kurd_cache_hits_total counter\n");
    output.push_str(&format!("kurd_cache_hits_total {}\n", cache_metrics.hits));

    output.push_str("# HELP kurd_cache_misses_total Tool cache misses\n");
    output.push_str("# TYPE kurd_cache_misses_total counter\n");
    output.push_str(&format!("kurd_cache_misses_total {}\n", cache_metrics.misses));

    output.push_str("# HELP kurd_cache_invalidations_total Tool cache invalidations\n");
    output.push_str("# TYPE kurd_cache_invalidations_total counter\n");
    output.push_str(&format!("kurd_cache_invalidations_total {}\n", cache_metrics.invalidations));

    // Upstream rejections
    let upstream_rejections = UPSTREAM_REJECTIONS.load(Ordering::Acquire);
    output.push_str("# HELP kurd_upstream_rejections_total Upstream call rejections due to concurrency limits\n");
    output.push_str("# TYPE kurd_upstream_rejections_total counter\n");
    output.push_str(&format!("kurd_upstream_rejections_total {}\n", upstream_rejections));

    // Upstream concurrency
    let upstream_active = UPSTREAM_ACTIVE_CALLS
        .lock()
        .map(|active| active.clone())
        .unwrap_or_default();

    let upstream_peaks = UPSTREAM_PEAK_CALLS
        .lock()
        .map(|peaks| peaks.clone())
        .unwrap_or_default();

    if !upstream_active.is_empty() {
        output.push_str("# HELP kurd_upstream_active_calls Active upstream calls\n");
        output.push_str("# TYPE kurd_upstream_active_calls gauge\n");

        for name in UPSTREAM_REGISTRY
            .read()
            .map(|registry| registry.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
        {
            let active = upstream_active.get(&name).copied().unwrap_or(0);
            output.push_str(&format!("kurd_upstream_active_calls {{upstream=\"{}\"}} {}\n", name, active));
        }
    }

    if !upstream_peaks.is_empty() {
        output.push_str("# HELP kurd_upstream_peak_active_calls Peak active upstream calls\n");
        output.push_str("# TYPE kurd_upstream_peak_active_calls gauge\n");

        for name in UPSTREAM_REGISTRY
            .read()
            .map(|registry| registry.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default()
        {
            let peak = upstream_peaks.get(&name).copied().unwrap_or(0);
            output.push_str(&format!("kurd_upstream_peak_active_calls {{upstream=\"{}\"}} {}\n", name, peak));
        }
    }

    // Latency histogram
    output.push_str("# HELP kurd_request_latency_histogram_ms Request latency histogram in milliseconds\n");
    output.push_str("# TYPE kurd_request_latency_histogram_ms histogram\n");
    let mut cumulative: u64 = 0;
    for (i, &bucket_ms) in LATENCY_BUCKETS.iter().enumerate() {
        cumulative += LATENCY_HISTOGRAM[i].load(Ordering::Acquire);
        output.push_str(&format!(
            "kurd_request_latency_histogram_ms_bucket {{le=\"{}\"}} {}\n",
            bucket_ms, cumulative
        ));
    }
    let inf_count = LATENCY_HISTOGRAM[LATENCY_BUCKETS.len()].load(Ordering::Acquire);
    cumulative += inf_count;
    output.push_str(&format!(
        "kurd_request_latency_histogram_ms_bucket {{le=\"+Inf\"}} {}\n",
        cumulative
    ));
    output.push_str(&format!(
        "kurd_request_latency_histogram_ms_count {}\n",
        completed_requests
    ));
    output.push_str(&format!(
        "kurd_request_latency_histogram_ms_sum {}\n",
        total_latency_ms
    ));

    // Response with Prometheus content type
    (
        [(
            "content-type",
            HeaderValue::from_static("text/plain; version=0.0.4"),
        )],
        output,
    )
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
            half_open: false,
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
                    "cached": entry.created_at.elapsed() < tools_cache_ttl(),
                    "ageMs": entry.created_at.elapsed().as_millis(),
                    "ttlMs": tools_cache_ttl().as_millis(),
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
                "ttlMs": tools_cache_ttl().as_millis(),
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
            "policyEnabled": POLICY_CALLBACK.read().map(|g| g.is_some()).unwrap_or(false),
            "policyDeniedRequests": POLICY_DENIED_REQUESTS.load(Ordering::Acquire),
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
            // async worker thread. A timeout prevents a hung coroutine from
            // exhausting the blocking pool indefinitely.
            let timeout_secs = UPSTREAM_TIMEOUT_MS.load(Ordering::Relaxed) as f64 / 1000.0;
            let result = future.call_method1("result", (timeout_secs,))?;

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

async fn mcp_root(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started_at = Instant::now();
    let request_id = request_trace_id(&headers);
    let body_bytes = body.len();
    let pre_parsed = serde_json::from_slice::<Value>(&body).ok();
    let method = pre_parsed.as_ref().and_then(|value| {
        value
            .get("method")
            .and_then(|method| method.as_str())
            .map(str::to_owned)
    });

    TOTAL_HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);

    let client_ip: String = headers
        .get("x-real-ip")
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| addr.ip().to_string());

    if !check_ip_allowlist(&client_ip) {
        REJECTED_HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
        let mut response = jsonrpc_error(
            StatusCode::FORBIDDEN,
            Value::Null,
            -32003,
            "IP not in allowlist",
            None,
        )
        .into_response();

        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert("x-request-id", value);
        }

        log_http_request(
            &request_id,
            method.as_deref(),
            response.status(),
            started_at.elapsed().as_millis() as u64,
            body_bytes,
        );
        return response;
    }

    if !check_rate_limit(&client_ip) {
        REJECTED_HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
        let mut response = jsonrpc_error(
            StatusCode::TOO_MANY_REQUESTS,
            Value::Null,
            RATE_LIMIT_ERROR_CODE,
            "Rate limit exceeded",
            Some(serde_json::json!({ "retryAfterMs": 1000 })),
        )
        .into_response();

        response.headers_mut().insert("retry-after", HeaderValue::from_static("1"));

        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert("x-request-id", value);
        }

        log_http_request(
            &request_id,
            method.as_deref(),
            response.status(),
            started_at.elapsed().as_millis() as u64,
            body_bytes,
        );
        return response;
    }

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

    let otel_start_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let (otel_trace_id, otel_parent_span_id) = headers.get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_traceparent)
        .map(|(tid, pid)| (tid, Some(pid)))
        .unwrap_or_else(|| (generate_hex_id(16), None));
    let otel_span_id = generate_hex_id(8);
    // Build the W3C traceparent for this gateway span, propagated to upstream calls.
    let current_traceparent = format!("00-{otel_trace_id}-{otel_span_id}-01");

    // Extract session-relevant fields before pre_parsed is consumed.
    let is_initialize = method.as_deref() == Some("initialize");
    let (session_client_info, session_client_caps) = if is_initialize {
        (
            pre_parsed.as_ref()
                .and_then(|v| v.get("params"))
                .and_then(|p| p.get("clientInfo"))
                .cloned()
                .unwrap_or(Value::Null),
            pre_parsed.as_ref()
                .and_then(|v| v.get("params"))
                .and_then(|p| p.get("capabilities"))
                .cloned()
                .unwrap_or(Value::Null),
        )
    } else {
        (Value::Null, Value::Null)
    };

    // JSON-RPC batch: array of request objects processed sequentially.
    let is_batch = matches!(&pre_parsed, Some(Value::Array(_)));
    let mut response = if is_batch {
        let batch_items = match pre_parsed {
            Some(Value::Array(items)) => items,
            _ => unreachable!(),
        };
        if batch_items.is_empty() {
            jsonrpc_error(StatusCode::OK, Value::Null, -32600, "Empty batch array", None)
                .into_response()
        } else {
            let mut batch_responses: Vec<Value> = Vec::with_capacity(batch_items.len());
            for item in &batch_items {
                let item_body = Bytes::from(item.to_string().into_bytes());
                let item_response = TASK_TRACEPARENT
                    .scope(
                        current_traceparent.clone(),
                        mcp_root_inner(headers.clone(), item_body, Some(item.clone())),
                    )
                    .await
                    .into_response();
                let item_bytes = axum::body::to_bytes(item_response.into_body(), MAX_MCP_BODY_BYTES)
                    .await
                    .unwrap_or_default();
                if let Ok(v) = serde_json::from_slice::<Value>(&item_bytes) {
                    batch_responses.push(v);
                }
            }
            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(Value::Array(batch_responses).to_string()))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    } else {
        TASK_TRACEPARENT
            .scope(
                current_traceparent.clone(),
                mcp_root_inner(headers, body, pre_parsed),
            )
            .await
            .into_response()
    };

    // For initialize: create a session and return Mcp-Session-Id in the response.
    if is_initialize {
        let session_id = generate_hex_id(16);
        if let Ok(mut sessions) = SESSION_REGISTRY.write() {
            // Simple high-watermark eviction to prevent unbounded growth.
            if sessions.len() >= 10_000 {
                sessions.clear();
            }
            sessions.insert(
                session_id.clone(),
                McpSession {
                    client_info: session_client_info,
                    client_capabilities: session_client_caps,
                },
            );
        }
        if let Ok(hv) = HeaderValue::from_str(&session_id) {
            response.headers_mut().insert("mcp-session-id", hv);
        }
    }

    let otel_end_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let otel_status_code = response.status().as_u16();
    let otel_op = method.as_deref().unwrap_or("mcp.request").to_string();
    {
        let tid = otel_trace_id.clone();
        let sid = otel_span_id.clone();
        let pid = otel_parent_span_id.clone();
        tokio::spawn(async move {
            export_otel_span(tid, sid, pid, otel_op, otel_start_ns, otel_end_ns, otel_status_code).await;
        });
    }
    let new_traceparent = format!("00-{otel_trace_id}-{otel_span_id}-01");
    if let Ok(tp_value) = HeaderValue::from_str(&new_traceparent) {
        response.headers_mut().insert("traceparent", tp_value);
    }

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }

    response.headers_mut().insert(
        "access-control-allow-origin",
        HeaderValue::from_static("*"),
    );

    let latency_ms = started_at.elapsed().as_millis() as u64;
    COMPLETED_HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
    TOTAL_HTTP_LATENCY_MS.fetch_add(latency_ms, Ordering::Relaxed);
    record_latency(latency_ms);
    log_http_request(
        &request_id,
        method.as_deref(),
        response.status(),
        latency_ms,
        body_bytes,
    );

    response
}

async fn mcp_root_inner(headers: HeaderMap, body: Bytes, pre_parsed: Option<Value>) -> impl IntoResponse {
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

    let parsed: Value = match pre_parsed {
        Some(value) => value,
        None => {
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
    // MCP: notifications — fire-and-forget, no JSON-RPC response
    // ---------------------------------------------------------
    if method.starts_with("notifications/") {
        return (
            StatusCode::ACCEPTED,
            [("content-type", "application/json")],
            String::new(),
        );
    }

    // ---------------------------------------------------------
    // MCP: initialize — required lifecycle handshake
    // ---------------------------------------------------------
    if method == "initialize" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {
                    "tools": { "listChanged": true },
                    "resources": { "subscribe": false, "listChanged": false },
                    "prompts": { "listChanged": false },
                    "logging": {}
                },
                "serverInfo": {
                    "name": "kurd",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "instructions": "Kurd is a high-performance MCP gateway powered by Rust."
            }
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            response.to_string(),
        );
    }

    // ---------------------------------------------------------
    // MCP: completion/complete — argument autocomplete stub
    // ---------------------------------------------------------
    if method == "completion/complete" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "completion": {
                    "values": [],
                    "hasMore": false
                }
            }
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            response.to_string(),
        );
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
                    "tools": {},
                    "resources": {},
                    "prompts": {}
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

        // Per-tenant tool filtering: only show tools the caller is allowed to see.
        {
            let filter_api_key = extract_bearer_token(&headers);
            let filter = tokio::task::spawn_blocking(move || {
                get_tenant_tool_filter(filter_api_key)
            })
            .await
            .unwrap_or(None);

            if let Some(ref patterns) = filter {
                tools.retain(|t| {
                    t.get("name")
                        .and_then(|n| n.as_str())
                        .map(|name| tool_name_matches_filter(name, patterns))
                        .unwrap_or(false)
                });
            }
        }

        // Count after tenant restrictions — this is what the caller is allowed to see.
        let available_count = tools.len();

        // Client-requested filtering: applied after tenant filter so tenant
        // restrictions cannot be bypassed.
        //
        // params.filter.namespace  — only tools in that upstream (e.g. "github")
        // params.filter.search     — case-insensitive substring in name or description
        {
            let client_filter = parsed.get("params").and_then(|p| p.get("filter"));

            if let Some(f) = client_filter {
                if let Some(ns) = f.get("namespace").and_then(|v| v.as_str()) {
                    let prefix = format!("{ns}.");
                    tools.retain(|t| {
                        t.get("name")
                            .and_then(|n| n.as_str())
                            .map(|name| name.starts_with(&prefix) || name == ns)
                            .unwrap_or(false)
                    });
                }

                if let Some(query) = f.get("search").and_then(|v| v.as_str()) {
                    let q = query.to_lowercase();
                    tools.retain(|t| {
                        let name_hit = t.get("name")
                            .and_then(|n| n.as_str())
                            .map(|n| n.to_lowercase().contains(&q))
                            .unwrap_or(false);
                        let desc_hit = t.get("description")
                            .and_then(|d| d.as_str())
                            .map(|d| d.to_lowercase().contains(&q))
                            .unwrap_or(false);
                        name_hit || desc_hit
                    });
                }
            }
        }

        let returned_count = tools.len();

        let offset: usize = parsed
            .get("params")
            .and_then(|p| p.get("cursor"))
            .and_then(|c| c.as_str())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0)
            .min(tools.len());

        let page: Vec<Value> = tools[offset..]
            .iter()
            .take(TOOLS_LIST_PAGE_SIZE)
            .cloned()
            .collect();

        let mut result_obj = serde_json::json!({
            "resultType": "complete",
            "tools": page,
            "ttlMs": MCP_LIST_TTL_MS,
            "cacheScope": MCP_CACHE_SCOPE,
            "_kurd": {
                "available": available_count,
                "returned": returned_count
            }
        });

        if offset + TOOLS_LIST_PAGE_SIZE < tools.len() {
            result_obj["nextCursor"] = Value::String(
                (offset + TOOLS_LIST_PAGE_SIZE).to_string()
            );
        }

        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result_obj
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
        // Policy check: verify caller is allowed to invoke this tool.
        // Runs before routing so both upstream and local tools are gated.
        // -----------------------------------------------------
        {
            let policy_api_key = extract_bearer_token(&headers);
            let policy_tool_name = tool_name.to_string();
            let denial = tokio::task::spawn_blocking(move || {
                check_policy_sync(policy_api_key, policy_tool_name)
            })
            .await
            .unwrap_or(None);

            if let Some(reason) = denial {
                POLICY_DENIED_REQUESTS.fetch_add(1, Ordering::Relaxed);
                return jsonrpc_error(
                    StatusCode::FORBIDDEN,
                    request_id,
                    POLICY_DENIED_ERROR_CODE,
                    "Forbidden by policy",
                    Some(serde_json::json!({ "reason": reason })),
                );
            }
        }

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
                // Build content array, respecting MCP multi-type content objects.
                // Tools may return: a plain string, a content object {type,…},
                // an array of content objects, or any other JSON value.
                let content: Vec<Value> = match serde_json::from_str::<Value>(&serialized) {
                    Ok(Value::String(s)) => {
                        vec![serde_json::json!({"type": "text", "text": s})]
                    }
                    Ok(Value::Object(obj)) if obj.contains_key("type") => {
                        // Already a content object (e.g. image, resource, audio).
                        vec![Value::Object(obj)]
                    }
                    Ok(Value::Array(arr))
                        if !arr.is_empty()
                            && arr.iter().all(|v| v.get("type").is_some()) =>
                    {
                        // Already an array of content objects.
                        arr
                    }
                    Ok(other) => {
                        vec![serde_json::json!({"type": "text", "text": other.to_string()})]
                    }
                    Err(_) => {
                        vec![serde_json::json!({"type": "text", "text": serialized})]
                    }
                };

                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "resultType": "complete",
                        "content": content,
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
    // MCP: resources/list
    // ---------------------------------------------------------
    if method == "resources/list" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "resultType": "complete",
                "resources": [],
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
    // MCP: prompts/list
    // ---------------------------------------------------------
    if method == "prompts/list" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "resultType": "complete",
                "prompts": [],
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
    // MCP: resources/read — gateway holds no resources; return empty content
    // ---------------------------------------------------------
    if method == "resources/read" {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "contents": []
            }
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            response.to_string(),
        );
    }

    // ---------------------------------------------------------
    // MCP: prompts/get — gateway holds no prompts; return not-found error
    // ---------------------------------------------------------
    if method == "prompts/get" {
        let name = parsed
            .get("params")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("<unknown>");

        let error = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32602,
                "message": format!("Prompt '{}' not found", name)
            }
        });

        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            error.to_string(),
        );
    }

    // ---------------------------------------------------------
    // MCP: logging/setLevel — accept, apply to tracing filter at runtime
    // ---------------------------------------------------------
    if method == "logging/setLevel" {
        let level = parsed
            .get("params")
            .and_then(|p| p.get("level"))
            .and_then(|l| l.as_str())
            .unwrap_or("info");

        // Apply the new level to the live tracing filter via the reload handle.
        let filter_str = match level {
            "debug" | "verbose" => "kurd=debug",
            "info"  | "notice"  => "kurd=info",
            "warning"           => "kurd=warn",
            "error" | "critical"| "alert" | "emergency" => "kurd=error",
            _                   => "kurd=info",
        };
        if let Ok(guard) = LOG_FILTER_HANDLE.lock() {
            if let Some(ref handle) = *guard {
                let new_filter = tracing_subscriber::EnvFilter::new(filter_str);
                let _ = handle.modify(|f| *f = new_filter);
            }
        }

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