"""Read API for archived events."""

from __future__ import annotations

from sillo import HttpContext, Query, Router, json, not_found

from app.models import EVENT_TYPES
from app.store import EventStore

router = Router(prefix="/api", tags=["logs"])


def _store(ctx: HttpContext) -> EventStore:
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
    event = await _store(ctx).get(event_id)
    if event is None:
        not_found(detail=f"event {event_id} was not found")
    return json({"event": event})


@router.get("/health", summary="Service health")
async def health(ctx: HttpContext):
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
