from kurd import Router
from kurd._kurd import start_http_gateway

router = Router()

@router.tool()
def add(a: int, b: int) -> int:
    return a + b

start_http_gateway("127.0.0.1:9200")
