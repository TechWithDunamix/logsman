"""Command-line entry point: ``python -m logsman`` or the ``logsman`` script.

Runs the ASGI application under uvicorn using the host, port and debug flag
from :mod:`logsman.core.config`. This is a convenience for local runs and simple
single-process deployments; production setups usually call uvicorn (or a
process manager such as systemd) directly so they can choose workers, proxy
headers and logging explicitly — see the README's production section.
"""

from __future__ import annotations

import argparse

import uvicorn

from logsman.core.config import config


def main(argv: list[str] | None = None) -> None:
    """Parse ``--host``/``--port``/``--reload`` and start uvicorn.

    Args:
        argv: Command-line arguments without the program name. ``None`` reads
            ``sys.argv`` as usual; tests pass an explicit list.

    Command-line flags override ``LOGSMAN_HOST``, ``LOGSMAN_PORT`` and
    ``LOGSMAN_DEBUG``. Reload is only enabled when explicitly requested, because
    auto-reload spawns a file watcher that has no place in production.
    """
    parser = argparse.ArgumentParser(prog="logsman", description="Run the Logsman server.")
    parser.add_argument("--host", default=config.host, help="interface to bind (LOGSMAN_HOST)")
    parser.add_argument("--port", type=int, default=config.port, help="port to bind (LOGSMAN_PORT)")
    parser.add_argument("--reload", action="store_true", help="restart on code changes (dev only)")
    args = parser.parse_args(argv)

    uvicorn.run(
        "logsman.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
