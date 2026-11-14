"""A process-local event store.

Used by the test suite and handy for trying the API without MongoDB. Data
lives in a dict and disappears with the process, so it is never suitable for
production.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from logsman.events.schema import EventModel


class MemoryEventStore:
    """An in-memory :class:`~logsman.storage.base.EventStore` with Mongo-like semantics.

    Mirrors :class:`~logsman.storage.mongo.MongoEventStore` where it matters to
    callers: first write wins per ``id``, listings are newest first, and
    returned documents are deep copies, so callers cannot mutate the store.

    Attributes:
        events: Stored documents keyed by log ``id``, in insertion order.
        opened: ``True`` between :meth:`open` and :meth:`close`; reported by
            :meth:`health`.
    """

    def __init__(self) -> None:
        """Start empty and closed."""
        self.events: dict[str, dict[str, Any]] = {}
        self.opened = False

    async def open(self) -> None:
        """Mark the store open."""
        self.opened = True

    async def close(self) -> None:
        """Mark the store closed. Stored events are kept."""
        self.opened = False

    async def health(self) -> bool:
        """Healthy exactly while open."""
        return self.opened

    async def put(self, event: EventModel, beacn: dict[str, Any]) -> None:
        """Store ``event`` unless its ``id`` is already present."""
        self.events.setdefault(
            event.id,
            {
                **event.model_dump(),
                "received_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "beacn": beacn,
            },
        )

    async def get(self, event_id: str) -> dict[str, Any] | None:
        """Return a copy of the stored document, or ``None``."""
        event = self.events.get(event_id)
        return deepcopy(event) if event else None

    async def list(
        self,
        *,
        event_type: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        tag: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Filter newest first (reverse insertion order), then slice ``offset:offset+limit``."""
        events = list(reversed(self.events.values()))
        if event_type:
            events = [item for item in events if item["type"] == event_type]
        if source:
            events = [item for item in events if item["source"] == source]
        if destination:
            events = [item for item in events if item["destination"] == destination]
        if tag:
            events = [item for item in events if tag in item["tags"]]
        return deepcopy(events[offset : offset + limit])
