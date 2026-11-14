"""The storage protocol shared by every event store.

Stores are structural (a :class:`typing.Protocol`): anything with these async
methods can be passed to :func:`logsman.core.app.create_app`.

Stored document shape (what ``get``/``list`` return)::

    {
      "id": "log_123", "type": "error", "source": "checkout",
      "destination": "payments", "data": {...}, "tags": ["payments"],
      "received_at": "2026-10-26T12:00:00.123000Z",   # when Logsman stored it
      "beacn": {"id": "evt_…", "event": "log.created", "topic": "logs", ...}
    }
"""

from __future__ import annotations

from typing import Any, Protocol

from logsman.events.schema import EventModel


class EventStore(Protocol):
    """Persistence contract for archived log events.

    Implementations must make :meth:`put` idempotent per ``event.id``: the
    first write wins and later writes with the same id are ignored. Listing is
    newest first by ``received_at``.
    """

    async def open(self) -> None:
        """Connect and prepare the store (indexes, schema). Raise to abort startup."""
        ...

    async def close(self) -> None:
        """Release connections. Called once on application shutdown."""
        ...

    async def health(self) -> bool:
        """Return ``True`` when the backing service answers, ``False`` otherwise. Never raise."""
        ...

    async def put(self, event: EventModel, beacn: dict[str, Any]) -> None:
        """Store ``event`` once, with the BEACN envelope metadata in ``beacn``.

        Args:
            event: The validated log payload.
            beacn: Envelope metadata (id, event name, topic, producer,
                environment, timestamp, severity, schema version, sequence).
        """
        ...

    async def get(self, event_id: str) -> dict[str, Any] | None:
        """Return the stored document for log ``event_id``, or ``None`` if absent."""
        ...

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
        """Return stored documents, newest first, matching every given filter.

        Args:
            event_type: Exact ``type`` match.
            source: Exact ``source`` match.
            destination: Exact ``destination`` match.
            tag: Documents whose ``tags`` contain this value.
            limit: Maximum number of documents to return.
            offset: Number of matching documents to skip (offset pagination).
        """
        ...
