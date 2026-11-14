"""Typed configuration for Logsman.

Every setting is read from an environment variable with the ``LOGSMAN_``
prefix, falling back to a ``.env`` file in the current working directory and
then to the defaults below. For example ``mongodb_url`` is read from
``LOGSMAN_MONGODB_URL``. Real environment variables always win over ``.env``.

The module exposes one ready-made instance, :data:`config`, which the ASGI
entry point uses. Tests build their own :class:`AppConfig` with
``_env_file=None`` so a developer's ``.env`` can never leak into a test run.
"""

from __future__ import annotations

from typing import Literal

from sillo.config import Config


class AppConfig(Config):
    """All Logsman settings, validated by Sillo's Pydantic-based ``Config``.

    Attributes:
        app_name: Title shown in the OpenAPI document.
        app_env: Deployment label. Informational only; nothing branches on it.
        debug: Enables Sillo's detailed error pages. Set ``false`` in
            production so stack traces are never returned to clients.
        host: Interface ``python -m logsman`` binds to.
        port: Port ``python -m logsman`` binds to.
        mongodb_url: Standard MongoDB connection string, e.g.
            ``mongodb://user:pass@db.internal:27017/?authSource=admin``.
        mongodb_database: Database that holds the archive.
        mongodb_collection: Collection that holds one document per log.
        beacn_url: Base URL of the BEACN server to consume from.
        beacn_api_key: BEACN API key with the ``events:read`` scope, in the
            same isolation environment as the publishers. Required whenever
            ``beacn_enabled`` is true.
        beacn_topic: BEACN topic to subscribe to.
        beacn_enabled: Start the background consumer. Set ``false`` to run
            the read API only (maintenance, read replicas, local UI work).
    """

    class Env:
        """Sillo ``Config`` options: read variables with the ``LOGSMAN_`` prefix."""

        env_prefix = "LOGSMAN_"

    app_name: str = "Logsman"
    app_env: Literal["local", "testing", "staging", "production"] = "local"
    debug: bool = True
    host: str = "127.0.0.1"
    port: int = 8010

    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "logsman"
    mongodb_collection: str = "events"

    beacn_url: str = "http://localhost:8000"
    beacn_api_key: str = ""
    beacn_topic: str = "logs"
    beacn_enabled: bool = True


#: Process-wide settings, loaded once at import from the environment and ``.env``.
config = AppConfig(_env_file=".env")
