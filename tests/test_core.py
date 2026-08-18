def test_fast_parse_exists():
    from kurd._kurd import fast_parse

    assert callable(fast_parse)


import pytest


def test_fast_parse_invalid_json():
    from kurd._kurd import fast_parse

    with pytest.raises(Exception):
        fast_parse('{"jsonrpc":')


def test_fast_parse_valid_json():
    from kurd._kurd import fast_parse

    method, request_id, params_json = fast_parse(
        '{"jsonrpc":"2.0","id":1,"method":"ping"}'
    )
    

    assert method == "ping"
    assert request_id == "1"

def test_fast_parse_with_params():
    from kurd._kurd import fast_parse

    method, request_id, params_json = fast_parse(
        '{"jsonrpc":"2.0","id":1,"method":"sum","params":{"a":2,"b":3}}'
    )

    assert method == "sum"
    assert request_id == "1"
    assert params_json == '{"a":2,"b":3}'


import asyncio

from kurd.router import Router


def test_router_dispatch_with_params():
    router = Router()

    @router.tool()
    async def add(a, b):
        return a + b

    result = asyncio.run(
        router.dispatch(
            '{"jsonrpc":"2.0","id":1,"method":"add","params":{"a":2,"b":3}}'
        )
    )

    assert '"result": 5' in result


def test_router_method_not_found():
    router = Router()

    result = asyncio.run(
        router.dispatch(
            '{"jsonrpc":"2.0","id":1,"method":"missing"}'
        )
    )

    assert '"code": -32601' in result

def test_router_invalid_params():
    router = Router()

    @router.tool()
    async def add(a, b):
        return a + b

    result = asyncio.run(
        router.dispatch(
            '{"jsonrpc":"2.0","id":1,"method":"add","params":{"a":2}}'
        )
    )

    assert '"code": -32602' in result

def test_router_invalid_json():
    router = Router()

    result = asyncio.run(
        router.dispatch(
            '{"jsonrpc":'
        )
    )

    assert '"code": -32700' in result   

def test_router_internal_error():
    router = Router()

    @router.tool()
    async def boom():
        raise RuntimeError("boom")

    result = asyncio.run(
        router.dispatch(
            '{"jsonrpc":"2.0","id":1,"method":"boom"}'
        )
    )

    assert '"code": -32603' in result

def test_async_tool_registration():
    router = Router()

    @router.tool(name="async_add")
    async def async_add(a: int, b: int):
        return a + b

    assert "async_add" in router._tools

def test_tool_schema_generation():
    router = Router()

    @router.tool(name="schema_test")
    async def schema_test(
        a: int,
        b: str = "x",
        tags: list[str] | None = None,
    ):
        return True

    assert "schema_test" in router._tools