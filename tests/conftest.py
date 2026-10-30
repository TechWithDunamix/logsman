from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import pytest

from app.config import AppConfig
from app.models import EventModel
from app.store import EventStore


class MemoryEventStore(EventStore):
    def __init__(self) -> None:
        self.events: dict[str, dict[str, Any]] = {}
        self.opened = False

    async def open(self) -> None:
        self.opened = True

    async def close(self) -> None:
        self.opened = False

    async def health(self) -> bool:
        return self.opened

    async def put(self, event: EventModel, beacn: dict[str, Any]) -> None:
        self.events.setdefault(
            event.id,
            {
                **event.model_dump(),
                "received_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "beacn": beacn,
            },
        )

    async def get(self, event_id: str) -> dict[str, Any] | None:
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


@pytest.fixture
def store() -> MemoryEventStore:
    return MemoryEventStore()


@pytest.fixture
def app(store):
    from app.bootstrap import create_app

    settings = AppConfig(
        _env_file=None,
        app_env="testing",
        debug=True,
        beacn_enabled=False,
    )
    return create_app(settings, store=store)


@pytest.fixture
def client(app):
    from sillo.testclient import TestClient

    with TestClient(app) as test_client:
        yield test_client
