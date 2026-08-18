import asyncio
import json
import inspect
from typing import get_args, get_origin, Union
import types
from typing import Callable, Dict, Any, Coroutine
from kurd._kurd import (
    fast_parse,
    register_tool,
    init_python_async_runtime,
)

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
                result = await tool_func(**params)
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