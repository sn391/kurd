"""kurd CLI — `kurd serve` starts the HTTP gateway."""

import argparse
import sys


def _cmd_serve(args: argparse.Namespace) -> None:
    import os
    from kurd._kurd import start_http_gateway, set_http_bearer_token

    # --token flag takes precedence over KURD_AUTH_TOKEN env var.
    token = args.token or os.environ.get("KURD_AUTH_TOKEN", "")
    if token:
        set_http_bearer_token(token)

    addr = f"{args.host}:{args.port}"
    print(f"Starting Kurd MCP gateway on {addr}", flush=True)

    # start_http_gateway blocks until stop_http_gateway() is called or the
    # process receives SIGINT/SIGTERM.
    start_http_gateway(addr)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="kurd",
        description="Kurd MCP gateway CLI",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    serve = sub.add_parser("serve", help="Start the HTTP MCP gateway")
    serve.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind host (default: 0.0.0.0)",
    )
    serve.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Bind port (default: 8000)",
    )
    serve.add_argument(
        "--token",
        default="",
        metavar="TOKEN",
        help="Bearer token for authentication (overrides KURD_AUTH_TOKEN env var)",
    )
    serve.set_defaults(func=_cmd_serve)

    parsed = parser.parse_args(argv)

    if not parsed.command:
        parser.print_help()
        sys.exit(1)

    parsed.func(parsed)


if __name__ == "__main__":
    main()
