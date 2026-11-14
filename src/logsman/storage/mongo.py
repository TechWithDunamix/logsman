"""MongoDB persistence for log events, using PyMongo's native async client."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.errors import PyMongoError

from logsman.events.schema import EventModel


class MongoEventStore:
    """An async MongoDB store with idempotent writes keyed by the log ``id``.

    One document per log. Writes use ``update_one(..., {"$setOnInsert": doc},
    upsert=True)`` so a redelivered or retried event never overwrites the first
    stored copy, and concurrent duplicates are resolved by the unique index.

    Indexes created by :meth:`open` (all idempotent):

    * ``id`` — unique; the archive key.
    * ``received_at`` descending — the default listing order.
    * ``(type, received_at)``, ``(source, received_at)``,
      ``(destination, received_at)`` — filtered listings stay index-backed.
    * ``tags`` — multikey index for tag filters.

    Args:
        url: MongoDB connection string. The client connects lazily; nothing
            touches the network until :meth:`open`.
        database: Database name.
        collection: Collection name.
    """

    def __init__(self, url: str, database: str, collection: str) -> None:
        """Create the client and resolve the database and collection (no network I/O yet)."""
        self._client = AsyncMongoClient(url)
        self._database = self._client[database]
        self._collection = self._database[collection]

    async def open(self) -> None:
        """Ping MongoDB and ensure every index exists.

        Raises:
            pymongo.errors.PyMongoError: MongoDB is unreachable, the credentials
                are wrong, or an index cannot be built (for example duplicate
                ids already present). The exception aborts application startup.
        """
        await self._database.command("ping")
        await self._collection.create_index([("id", ASCENDING)], unique=True)
        await self._collection.create_index([("received_at", DESCENDING)])
        await self._collection.create_index([("type", ASCENDING), ("received_at", DESCENDING)])
        await self._collection.create_index([("source", ASCENDING), ("received_at", DESCENDING)])
        await self._collection.create_index(
            [("destination", ASCENDING), ("received_at", DESCENDING)]
        )
        await self._collection.create_index([("tags", ASCENDING)])

    async def close(self) -> None:
        """Close the client and its connection pool."""
        await self._client.close()

    async def health(self) -> bool:
        """Return whether a ``ping`` command succeeds right now."""
        try:
            await self._database.command("ping")
            return True
        except PyMongoError:
            return False

    async def put(self, event: EventModel, beacn: dict[str, Any]) -> None:
        """Insert ``event`` unless a document with the same ``id`` already exists.

        ``received_at`` is stamped with the current UTC time and the BEACN
        envelope metadata is stored under ``beacn``. Both are written only on
        insert, so they describe the first delivery.
        """
        document = event.model_dump(mode="python")
        document["received_at"] = datetime.now(UTC)
        document["beacn"] = beacn
        await self._collection.update_one(
            {"id": event.id},
            {"$setOnInsert": document},
            upsert=True,
        )

    async def get(self, event_id: str) -> dict[str, Any] | None:
        """Fetch one log by its payload ``id`` (Mongo's ``_id`` is never returned)."""
        document = await self._collection.find_one({"id": event_id}, {"_id": 0})
        return json_ready(document) if document else None

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
        """List logs newest first; see :meth:`EventStore.list` for the filters.

        Ties on ``received_at`` are broken by ``id`` descending so pages are
        stable. Offset pagination is fine for an operator UI; very deep offsets
        get slower because MongoDB still walks the skipped documents.
        """
        query: dict[str, Any] = {}
        if event_type:
            query["type"] = event_type
        if source:
            query["source"] = source
        if destination:
            query["destination"] = destination
        if tag:
            query["tags"] = tag

        cursor = (
            self._collection.find(query, {"_id": 0})
            .sort([("received_at", DESCENDING), ("id", DESCENDING)])
            .skip(offset)
            .limit(limit)
        )
        return [json_ready(document) for document in await cursor.to_list(length=limit)]


def json_ready(document: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``document`` whose ``received_at`` is an ISO-8601 string.

    MongoDB returns naive ``datetime`` objects (UTC by convention). They are
    marked as UTC and rendered with a ``Z`` suffix, e.g.
    ``2026-10-26T12:00:00.123000Z``, so API responses are plain JSON.
    """
    result = dict(document)
    received_at = result.get("received_at")
    if isinstance(received_at, datetime):
        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=UTC)
        result["received_at"] = received_at.isoformat().replace("+00:00", "Z")
    return result
