"""Application assembly and dependency lifecycle.

:func:`create_app` is the single place where Logsman's pieces are wired
together: it builds the Sillo application, chooses the event store, optionally
creates the BEACN consumer, registers startup/shutdown hooks and mounts the
HTTP routes. Everything is injectable so tests can run without MongoDB or a
BEACN server.
"""

from __future__ import annotations

import asyncio
import logging

from sillo import SilloApp

from logsman import __version__
from logsman.api.routes import router
from logsman.consumer.listener import BeacnLogListener
from logsman.core.config import AppConfig, config
from logsman.storage.base import EventStore
from logsman.storage.mongo import MongoEventStore

logger = logging.getLogger("logsman")


def create_app(
    settings: AppConfig | None = None,
    *,
    store: EventStore | None = None,
    listener: BeacnLogListener | None = None,
) -> SilloApp:
    """Build a fully wired Logsman application.

    Args:
        settings: Configuration to use. Defaults to the process-wide
            :data:`logsman.core.config.config`.
        store: Event store to use. Defaults to a :class:`MongoEventStore` built
            from ``settings``; tests pass an in-memory store.
        listener: Consumer to use when ``settings.beacn_enabled`` is true.
            Defaults to a :class:`BeacnLogListener` built from ``settings``.

    Returns:
        A :class:`sillo.SilloApp` with the store at ``app.state["event_store"]``,
        the topic at ``app.state["beacn_topic"]`` and, when enabled, the consumer
        at ``app.state["beacn_listener"]``.

    Raises:
        ValueError: The consumer is enabled but ``LOGSMAN_BEACN_API_KEY`` is
            empty. Failing at build time is deliberate: a consumer without
            credentials would otherwise retry forever in the background while
            the health check looked fine.

    Lifecycle:
        * **startup** opens the store (pings MongoDB and creates indexes — a
          failure here aborts startup) and then starts the consumer thread.
        * **shutdown** signals the consumer to stop, waits briefly for its
          thread off the event loop, then closes the store.
    """
    settings = settings or config
    application = SilloApp(
        debug=settings.debug,
        title=settings.app_name,
        version=__version__,
    )

    event_store = store or MongoEventStore(
        settings.mongodb_url,
        settings.mongodb_database,
        settings.mongodb_collection,
    )
    application.state["event_store"] = event_store
    application.state["beacn_topic"] = settings.beacn_topic

    if settings.beacn_enabled:
        if not settings.beacn_api_key:
            raise ValueError("LOGSMAN_BEACN_API_KEY is required when LOGSMAN_BEACN_ENABLED=true")
        listener = listener or BeacnLogListener(
            url=settings.beacn_url,
            api_key=settings.beacn_api_key,
            topic=settings.beacn_topic,
            store=event_store,
        )
        application.state["beacn_listener"] = listener

    async def startup() -> None:
        """Open the store, then start the consumer on the running event loop."""
        await event_store.open()
        active_listener = application.state.get("beacn_listener")
        if active_listener is not None:
            active_listener.start(asyncio.get_running_loop())
            logger.info(
                "consuming BEACN topic %r from %s", settings.beacn_topic, settings.beacn_url
            )
        else:
            logger.info("BEACN consumer disabled; serving the read API only")

    async def shutdown() -> None:
        """Stop the consumer thread (without blocking the loop) and close the store."""
        active_listener = application.state.get("beacn_listener")
        if active_listener is not None:
            active_listener.stop()
            await asyncio.to_thread(active_listener.join)
        await event_store.close()

    application.on_startup(startup)
    application.on_shutdown(shutdown)
    application.mount_router(router)
    return application
