"""MongoDB persistence for log events."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.errors import PyMongoError

from app.models import EventModel


class EventStore(Protocol):
    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def health(self) -> bool: ...

    async def put(self, event: EventModel, beacn: dict[str, Any]) -> None: ...

    async def get(self, event_id: str) -> dict[str, Any] | None: ...

    async def list(
        self,
        *,
        event_type: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        tag: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]: ...


class MongoEventStore:
    """An async Mongo store with idempotent writes keyed by event id."""

    def __init__(self, url: str, database: str, collection: str) -> None:
        self._client = AsyncMongoClient(url)
        self._database = self._client[database]
        self._collection = self._database[collection]

    async def open(self) -> None:
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
        await self._client.close()

    async def health(self) -> bool:
        try:
            await self._database.command("ping")
            return True
        except PyMongoError:
            return False

    async def put(self, event: EventModel, beacn: dict[str, Any]) -> None:
        document = event.model_dump(mode="python")
        document["received_at"] = datetime.now(UTC)
        document["beacn"] = beacn
        await self._collection.update_one(
            {"id": event.id},
            {"$setOnInsert": document},
            upsert=True,
        )

    async def get(self, event_id: str) -> dict[str, Any] | None:
        document = await self._collection.find_one({"id": event_id}, {"_id": 0})
        return _json_ready(document) if document else None

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
        return [_json_ready(document) for document in await cursor.to_list(length=limit)]


def _json_ready(document: dict[str, Any]) -> dict[str, Any]:
    """Return a copy whose Mongo timestamps are regular JSON strings."""
    result = dict(document)
    received_at = result.get("received_at")
    if isinstance(received_at, datetime):
        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=UTC)
        result["received_at"] = received_at.isoformat().replace("+00:00", "Z")
    return result
