from __future__ import annotations

from app.models import EventModel


async def _seed(store) -> None:
    await store.put(
        EventModel(
            type="error",
            source="checkout",
            destination="payments",
            id="log_1",
            data={"message": "card declined"},
            tags=["payments", "urgent"],
        ),
        {"id": "evt_1", "topic": "logs"},
    )
    await store.put(
        EventModel(
            type="info",
            source="worker",
            destination="email",
            id="log_2",
            data={"message": "sent"},
            tags=["email"],
        ),
        {"id": "evt_2", "topic": "logs"},
    )


def test_health(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["checks"] == {"mongodb": "ok", "beacn_listener": "ok"}
    assert response.json()["topic"] == "logs"


def test_list_and_filter_events(client, store):
    client.portal.call(_seed, store)

    response = client.get("/api/events?type=error&tag=urgent")

    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["events"][0]["id"] == "log_1"


def test_rejects_unknown_event_type(client):
    response = client.get("/api/events?type=warning")
    assert response.status_code == 422


def test_get_one_and_not_found(client, store):
    client.portal.call(_seed, store)

    response = client.get("/api/events/log_2")
    assert response.status_code == 200
    assert response.json()["event"]["destination"] == "email"

    missing = client.get("/api/events/missing")
    assert missing.status_code == 404


def test_openapi_lists_read_endpoints(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert "/api/events" in paths
    assert "/api/events/{event_id}" in paths
