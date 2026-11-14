"""Read API for archived events.

All routes live under ``/api``:

==========================  ================================================
``GET /api/events``         List logs, newest first, with optional filters
``GET /api/events/{id}``    One log by its payload ``id``
``GET /api/health``         MongoDB and consumer health (200 or 503)
==========================  ================================================

The API is read-only and has **no authentication**; keep it on a private
network or behind an authenticating reverse proxy. Sillo also serves the
OpenAPI document at ``/openapi.json``.
"""

from __future__ import annotations

from sillo import HttpContext, Query, Router, json, not_found

from logsman.events.schema import EVENT_TYPES
from logsman.storage.base import EventStore

router = Router(prefix="/api", tags=["logs"])


def _store(ctx: HttpContext) -> EventStore:
    """Return the event store that :func:`~logsman.core.app.create_app` put on app state."""
    return ctx.base_app.state["event_store"]


@router.get("/events", summary="Read archived log events")
async def list_events(
    ctx: HttpContext,
    event_type=Query(None, type=str, alias="type"),
    source=Query(None, type=str),
    destination=Query(None, type=str),
    tag=Query(None, type=str),
    limit=Query(50, type=int, ge=1, le=200),
    offset=Query(0, type=int, ge=0),
):
    """List archived logs, newest first.

    Query parameters (all optional, combined with AND):

    * ``type`` — one of ``error``, ``info``, ``debug``, ``actions``.
    * ``source`` / ``destination`` — exact match.
    * ``tag`` — logs carrying this tag.
    * ``limit`` — 1 to 200, default 50. ``offset`` — default 0.

    Returns ``{"events": [...], "count": n, "limit": l, "offset": o}`` where
    ``count`` is the size of this page, not the total. An unknown ``type``
    returns 422.
    """
    if event_type is not None and event_type not in EVENT_TYPES:
        return json(
            {"detail": "type must be one of: error, info, debug, actions"},
            status_code=422,
        )
    events = await _store(ctx).list(
        event_type=event_type,
        source=source,
        destination=destination,
        tag=tag,
        limit=limit,
        offset=offset,
    )
    return json({"events": events, "count": len(events), "limit": limit, "offset": offset})


@router.get("/events/{event_id}", summary="Read one archived log event")
async def get_event(ctx: HttpContext, event_id: str):
    """Return ``{"event": {...}}`` for the log whose payload ``id`` is ``event_id``.

    Responds 404 when no such log has been archived.
    """
    event = await _store(ctx).get(event_id)
    if event is None:
        not_found(detail=f"event {event_id} was not found")
    return json({"event": event})


@router.get("/health", summary="Service health")
async def health(ctx: HttpContext):
    """Report MongoDB reachability and whether the consumer thread is alive.

    Returns 200 with ``"status": "ok"`` when every check passes and 503 with
    ``"status": "degraded"`` otherwise, so it can back load-balancer and
    systemd/uptime probes directly. With the consumer disabled, the
    ``beacn_listener`` check reports ``ok``.
    """
    mongo_ok = await _store(ctx).health()
    listener = ctx.base_app.state.get("beacn_listener")
    listener_ok = listener is None or listener.running
    healthy = mongo_ok and listener_ok
    return json(
        {
            "status": "ok" if healthy else "degraded",
            "checks": {
                "mongodb": "ok" if mongo_ok else "unavailable",
                "beacn_listener": "ok" if listener_ok else "stopped",
            },
            "topic": ctx.base_app.state["beacn_topic"],
        },
        status_code=200 if healthy else 503,
    )
