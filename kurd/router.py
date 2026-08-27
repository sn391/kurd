import asyncio
import json
import inspect
from dataclasses import dataclass, field
from typing import get_args, get_origin, Union, Optional
import types
from typing import Callable, Dict, Any, Coroutine
from kurd._kurd import (
    fast_parse,
    register_tool,
    unregister_tool as _rust_unregister_tool,
    register_upstream,
    unregister_upstream,
    clear_tools_cache,
    set_runtime_limits,
    set_request_logging,
    set_rate_limiting,
    set_tools_cache_ttl_ms,
    set_upstream_timeout_ms as _rust_set_upstream_timeout_ms,
    runtime_status,
    init_python_async_runtime,
    list_upstreams as _rust_list_upstreams,
    list_tools as _rust_list_tools,
    set_ip_allowlist as _rust_set_ip_allowlist,
    clear_ip_allowlist as _rust_clear_ip_allowlist,
)


@dataclass
class RuntimeConfig:
    """All runtime options for a Kurd gateway in one place."""
    global_concurrency: int = 512
    upstream_concurrency: int = 64
    python_concurrency: int = 64
    request_logging: bool = False
    rate_limiting_enabled: bool = False
    rate_limit_per_ip_rps: int = 1000
    rate_limit_global_rps: int = 10_000
    tools_cache_ttl_ms: int = 30_000
    enable_dlq: bool = False
    enable_idempotency: bool = False
    secrets_backend: str = "env"
    dlq_storage_path: Optional[str] = None
    idempotency_storage_path: Optional[str] = None
    enable_webhooks: bool = False
    enable_distributed_state: bool = False
    distributed_state_backend: str = "memory"
    redis_url: str = "redis://localhost:6379/0"
    enable_distributed_tracing: bool = False
    ip_allowlist: Optional[list] = None
    upstream_timeout_ms: int = 30_000
from kurd.dead_letter_queue import DeadLetterQueue
from kurd.idempotency import IdempotencyManager
from kurd.secrets_management import SecretsManager
from kurd.webhooks import WebhookManager
from kurd.distributed_tracing import TracingContext, extract_context
from kurd.distributed_state import DistributedStateManager


def _python_type_to_schema(annotation):
    origin = get_origin(annotation)
    args = get_args(annotation)

    if annotation is int:
        return {"type": "integer"}

    if annotation is float:
        return {"type": "number"}

    if annotation is bool:
        return {"type": "boolean"}

    if annotation is str:
        return {"type": "string"}

    if origin is list:
        item_type = args[0] if args else str
        return {
            "type": "array",
            "items": _python_type_to_schema(item_type),
        }

    if origin is dict:
        return {
            "type": "object"
        }

    if origin in (Union, types.UnionType):
        non_none = [
            arg
            for arg in args
            if arg is not type(None)
        ]

        if len(non_none) == 1:
            return _python_type_to_schema(non_none[0])

    return {"type": "string"}


class Router:
    def __init__(self):
        self._tools: Dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}
        init_python_async_runtime()
        self.dlq: DeadLetterQueue = None
        self.idempotency: IdempotencyManager = None
        self.secrets: SecretsManager = None
        self.webhooks: WebhookManager = None
        self.distributed_state: DistributedStateManager = None
        self.current_trace: TracingContext = None

    def mount(self, name: str, url: str) -> None:
        if not name:
            raise ValueError("Upstream name cannot be empty")

        if not url:
            raise ValueError("Upstream URL cannot be empty")

        register_upstream(name, url)

    def unmount(self, name: str) -> bool:
        if not name:
            raise ValueError("Upstream name cannot be empty")

        return unregister_upstream(name)

    def refresh_tools(self) -> None:
        clear_tools_cache()

    def reload_tool(self, name: str, func: Callable) -> None:
        """Hot-reload a tool without restarting the gateway."""
        signature = inspect.signature(func)

        properties = {}
        required = []

        for param_name, param in signature.parameters.items():
            annotation = param.annotation

            properties[param_name] = _python_type_to_schema(annotation)
            if param.default is not inspect.Parameter.empty:
                properties[param_name]["default"] = param.default

            if param.default is inspect.Parameter.empty:
                required.append(param_name)

        input_schema = {
            "type": "object",
            "properties": properties,
            "required": required,
        }

        # Update tool in registry
        self._tools[name] = func
        register_tool(
            name,
            func.__doc__ or "",
            json.dumps(input_schema),
            func,
        )

        # Clear cache so new schema is used
        self.refresh_tools()

    def unregister_tool(self, name: str) -> bool:
        """Remove a tool from the gateway (Python dict and Rust registry)."""
        if name not in self._tools:
            return False
        del self._tools[name]
        _rust_unregister_tool(name)
        return True

    def configure_runtime(
        self,
        config: RuntimeConfig | None = None,
        **kwargs,
    ) -> None:
        """Configure runtime. Pass a RuntimeConfig object or keyword arguments."""
        if config is None:
            config = RuntimeConfig(**kwargs)

        set_runtime_limits(
            config.global_concurrency,
            config.upstream_concurrency,
            config.python_concurrency,
        )
        set_request_logging(config.request_logging)
        set_rate_limiting(
            config.rate_limiting_enabled,
            config.rate_limit_per_ip_rps,
            config.rate_limit_global_rps,
        )
        set_tools_cache_ttl_ms(config.tools_cache_ttl_ms)
        _rust_set_upstream_timeout_ms(config.upstream_timeout_ms)

        if config.ip_allowlist is not None:
            _rust_set_ip_allowlist([str(ip) for ip in config.ip_allowlist])
        else:
            _rust_clear_ip_allowlist()

        if config.enable_dlq:
            self.dlq = DeadLetterQueue(storage_path=config.dlq_storage_path)

        if config.enable_idempotency:
            self.idempotency = IdempotencyManager(storage_path=config.idempotency_storage_path)

        self.secrets = SecretsManager(backend=config.secrets_backend)

        if config.enable_webhooks:
            self.webhooks = WebhookManager()

        if config.enable_distributed_state:
            self.distributed_state = DistributedStateManager(
                backend=config.distributed_state_backend,
                redis_url=config.redis_url if config.distributed_state_backend == "redis" else None,
            )

        if config.enable_distributed_tracing:
            self.current_trace = TracingContext()

    def list_upstreams(self) -> list[tuple[str, str]]:
        """Return [(name, url)] for every registered upstream."""
        return _rust_list_upstreams()

    def list_tools(self) -> list[str]:
        """Return names of all locally registered tools."""
        return _rust_list_tools()

    def get_dlq(self) -> DeadLetterQueue:
        """Get DLQ manager instance."""
        if not self.dlq:
            self.dlq = DeadLetterQueue()
        return self.dlq

    def get_idempotency(self) -> IdempotencyManager:
        """Get idempotency manager instance."""
        if not self.idempotency:
            self.idempotency = IdempotencyManager()
        return self.idempotency

    def get_secrets(self) -> SecretsManager:
        """Get secrets manager instance."""
        if not self.secrets:
            self.secrets = SecretsManager(backend="env")
        return self.secrets

    def get_webhooks(self) -> WebhookManager:
        """Get webhooks manager instance."""
        if not self.webhooks:
            self.webhooks = WebhookManager()
        return self.webhooks

    def get_distributed_state(self) -> DistributedStateManager:
        """Get distributed state manager instance."""
        if not self.distributed_state:
            self.distributed_state = DistributedStateManager(backend="memory")
        return self.distributed_state

    def get_tracing_context(self) -> TracingContext:
        """Get current tracing context."""
        if not self.current_trace:
            self.current_trace = TracingContext()
        return self.current_trace

    def set_tracing_context(self, headers: Dict[str, str]) -> TracingContext:
        """Extract and set tracing context from request headers."""
        self.current_trace = extract_context(headers)
        return self.current_trace

    def runtime_status(self) -> dict:
        """Return a lightweight snapshot of runtime/backpressure counters."""
        status = dict(runtime_status())

        if self.dlq:
            status["dlq"] = self.dlq.to_json()

        if self.idempotency:
            status["idempotency"] = self.idempotency.to_json()

        if self.secrets:
            status["secrets"] = self.secrets.to_json()

        if self.webhooks:
            status["webhooks"] = self.webhooks.to_json()

        if self.distributed_state:
            status["distributed_state"] = self.distributed_state.to_json()

        if self.current_trace:
            status["current_trace"] = {
                "trace_id": self.current_trace.trace_id,
                "span_count": len(self.current_trace.spans),
            }

        return status

    def tool(self, name: str = None):
        """Decorator to register asynchronous tools."""
        def decorator(func: Callable[..., Coroutine[Any, Any, Any]]):
            tool_name = name or func.__name__
            self._tools[tool_name] = func
            signature = inspect.signature(func)

            properties = {}
            required = []

            for param_name, param in signature.parameters.items():
                annotation = param.annotation

                properties[param_name] = _python_type_to_schema(annotation)
                if param.default is not inspect.Parameter.empty:
                    properties[param_name]["default"] = param.default

                if param.default is inspect.Parameter.empty:
                    required.append(param_name)

            input_schema = {
                "type": "object",
                "properties": properties,
                "required": required,
            }
            register_tool(
                tool_name,
                func.__doc__ or "",
                json.dumps(input_schema),
                func,
            )
            return func
        return decorator

    async def dispatch(self, raw_json_rpc_payload: str) -> str:
        """Dispatches incoming JSON-RPC payloads with advanced error handling."""
        req_id = None
        try:
            # Step 1: Parse and validate via Rust core
            try:
                method, request_id, params_json = fast_parse(raw_json_rpc_payload)
            except Exception:
                return json.dumps({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32700,
                        "message": "Parse error: Invalid JSON was received by the server."
                    },
                    "id": None
                })

            params = json.loads(params_json) if params_json else {}
            payload = {"method": method, "id": request_id, "params": params}
            method = payload.get("method")
            params = payload.get("params", {})
            req_id = payload.get("id")

            # Check if method exists
            if not method or method not in self._tools:
                return json.dumps({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: '{method}'"
                    },
                    "id": req_id
                })

            # Step 2: Execute tool with parameter validation
            tool_func = self._tools[method]
            try:
                result = tool_func(**params)
                if inspect.isawaitable(result):
                    result = await result
            except TypeError as te:
                return json.dumps({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32602,
                        "message": "Invalid params",
                        "data": str(te)
                    },
                    "id": req_id
                })
            except Exception as ex:
                return json.dumps({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32603,
                        "message": "Internal error",
                        "data": str(ex)
                    },
                    "id": req_id
                })

            return json.dumps({
                "jsonrpc": "2.0",
                "result": result,
                "id": req_id
            })

        except Exception as e:
            return json.dumps({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": "Server error",
                    "data": str(e)
                },
                "id": req_id
            })