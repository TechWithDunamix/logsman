"""Application assembly and dependency lifecycle."""

from __future__ import annotations

import asyncio

from sillo import SilloApp

from app.config import AppConfig, config
from app.listener import BeacnLogListener
from app.store import EventStore, MongoEventStore


def create_app(
    settings: AppConfig | None = None,
    *,
    store: EventStore | None = None,
    listener: BeacnLogListener | None = None,
) -> SilloApp:
    settings = settings or config
    application = SilloApp(
        debug=settings.debug,
        title=settings.app_name,
        version="0.1.0",
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
        print("starting up......")
        await event_store.open()
        active_listener = application.state.get("beacn_listener")
        print("active listiner",active_listener)
        if active_listener is not None:
            active_listener.start(asyncio.get_running_loop())
            print("connection to beacn success")

    async def shutdown() -> None:
        active_listener = application.state.get("beacn_listener")
        if active_listener is not None:
            active_listener.stop()
            await asyncio.to_thread(active_listener.join)
        await event_store.close()

    application.on_startup(startup)
    application.on_shutdown(shutdown)

    from routes.api import router

    application.mount_router(router)
    return application
