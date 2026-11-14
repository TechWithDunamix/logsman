"""Logsman — a BEACN log archive with a read-only HTTP API.

Logsman subscribes to one BEACN topic (``logs`` by default), validates every
event payload against a strict log contract, stores each valid log once in
MongoDB and serves the archive through a small JSON API.

Package layout::

    logsman/
        main.py          ASGI entry point (``uvicorn logsman.main:app``)
        __main__.py      ``python -m logsman`` / the ``logsman`` command
        core/            configuration and application assembly
        events/          the log payload contract (Pydantic model)
        storage/         the storage protocol, MongoDB and in-memory stores
        consumer/        the background BEACN SSE consumer thread
        api/             HTTP routes (events and health)
        vendor/          the bundled BEACN Python SDK

Data flow::

    publisher ─► BEACN topic ─► consumer.BeacnLogListener ─► storage.MongoEventStore
                                                                  │
                                              api.routes ◄────────┘
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
