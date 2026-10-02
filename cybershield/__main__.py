"""Command line entry points.

    python -m cybershield api     [--host 0.0.0.0] [--port 8000] [--reload]
    python -m cybershield worker  [--concurrency 2] [--metrics-port 9100]

Production runs one process per container and scales by adding replicas:
API nodes with ``CS_RUN_WORKER=false`` and a separate pool of workers.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal

from .config import Settings


def run_api(args) -> None:
    import uvicorn

    uvicorn.run("app:app", host=args.host, port=args.port, reload=args.reload, proxy_headers=True,
                forwarded_allow_ips="*", log_level="info")


async def run_worker(args) -> None:
    from prometheus_client import start_http_server

    from .main import configure_logging
    from .services import Runtime

    settings = Settings()
    configure_logging(settings.log_json)
    runtime = Runtime(settings)
    start_http_server(args.metrics_port)
    await runtime.start(api=False, workers=args.concurrency)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # not available on Windows
            loop.add_signal_handler(sig, stop.set)
    logging.getLogger(__name__).info("worker pool running (%d), metrics on :%d", args.concurrency, args.metrics_port)
    try:
        await stop.wait()
    finally:
        await runtime.stop()


def main() -> None:
    parser = argparse.ArgumentParser(prog="cybershield")
    sub = parser.add_subparsers(dest="command", required=True)
    api = sub.add_parser("api", help="run the HTTP/WebSocket API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--reload", action="store_true")
    worker = sub.add_parser("worker", help="run stream workers")
    worker.add_argument("--concurrency", type=int, default=1)
    worker.add_argument("--metrics-port", type=int, default=9100)
    args = parser.parse_args()

    if args.command == "api":
        run_api(args)
    else:
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(run_worker(args))


if __name__ == "__main__":
    main()
