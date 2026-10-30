from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.listener import BeacnLogListener


def _envelope(data):
    return SimpleNamespace(
        id="evt_1",
        event="log.created",
        topic="logs",
        producer="test-suite",
        environment="testing",
        timestamp="2026-10-26T12:00:00Z",
        severity="info",
        schema_version="1",
        seq=1,
        data=data,
    )


async def test_listener_persists_valid_beacn_payload(store):
    listener = BeacnLogListener(
        url="http://beacn.test",
        api_key="bk_test",
        topic="logs",
        store=store,
    )
    listener._loop = asyncio.get_running_loop()

    await asyncio.to_thread(
        listener._persist,
        _envelope(
            {
                "type": "debug",
                "source": "api",
                "destination": "worker",
                "id": "log_1",
                "data": {"attempt": 2},
                "tags": ["jobs"],
            }
        ),
    )

    assert store.events["log_1"]["beacn"]["id"] == "evt_1"


async def test_listener_discards_invalid_beacn_payload(store):
    listener = BeacnLogListener(
        url="http://beacn.test",
        api_key="bk_test",
        topic="logs",
        store=store,
    )
    listener._loop = asyncio.get_running_loop()

    await asyncio.to_thread(
        listener._persist,
        _envelope(
            {
                "type": "warning",
                "source": "api",
                "destination": "worker",
                "id": "log_bad",
            }
        ),
    )

    assert "log_bad" not in store.events
    assert listener.last_error is not None
