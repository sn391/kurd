import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import TCPServer

class FastThreadingHTTPServer(ThreadingHTTPServer):
    """Threading HTTP server that avoids hostname/FQDN lookup during bind."""

    def server_bind(self):
        TCPServer.server_bind(self)
        host, port = self.server_address
        self.server_name = host
        self.server_port = port



def build_handler(tool_name: str, delay: float, call_delay: float = 0.0):
    counters = {"tools_list": 0}
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("content-length", "0"))
            body = self.rfile.read(length)

            try:
                request = json.loads(body)
            except json.JSONDecodeError:
                request = {}

            method = request.get("method")
            request_id = request.get("id")

            if method == "tools/list":
                counters["tools_list"] += 1

                if delay:
                    time.sleep(delay)

                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "tools": [
                            {
                                "name": tool_name,
                                "description": f"Test tool: {tool_name}",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "a": {"type": "integer"},
                                        "b": {"type": "integer"},
                                    },
                                    "required": ["a", "b"],
                                },
                            }
                        ]
                    },
                }

            elif method == "test/counter":
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "toolsListCalls": counters["tools_list"]
                    },
                }

            elif method == "tools/call":
                if call_delay:
                    time.sleep(call_delay)

                params = request.get("params", {})
                name = params.get("name")
                arguments = params.get("arguments", {})

                if name == tool_name:
                    result = arguments["a"] + arguments["b"]

                    response = {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": str(result),
                                }
                            ],
                            "isError": False,
                        },
                    }
                else:
                    response = {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {
                            "code": -32601,
                            "message": "Tool not found",
                        },
                    }

            else:
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": "Method not found",
                    },
                }

            encoded = json.dumps(response).encode()

            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):
            pass

    return Handler


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--tool-name", default="add")
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--call-delay", type=float, default=0.0)
    args = parser.parse_args()

    handler = build_handler(
        args.tool_name,
        args.delay,
        args.call_delay,
    )
    try:
        server = FastThreadingHTTPServer(("127.0.0.1", args.port), handler)
        print(f"Server started on http://127.0.0.1:{args.port}", flush=True)
        server.serve_forever()
    except OSError as e:
        print(f"Failed to start server on port {args.port}: {e}", flush=True)
        raise