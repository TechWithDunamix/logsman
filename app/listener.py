"""Background BEACN consumer."""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any

from pydantic import ValidationError

from app.models import EventModel
from app.store import EventStore
from app.vendor.beacn import Beacn

logger = logging.getLogger("logsman.listener")


class BeacnLogListener:
    """Consume one BEACN topic in a daemon thread and persist valid payloads."""

    def __init__(
        self,
        *,
        url: str,
        api_key: str,
        topic: str,
        store: EventStore,
        retry_delay: float = 5.0,
        write_timeout: float = 30.0,
    ) -> None:
        self._client = Beacn(url=url, api_key=api_key)
        self._topic = topic
        self._store = store
        self._retry_delay = retry_delay
        self._write_timeout = write_timeout
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self.last_event_id: str | None = None
        self.last_error: str | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        if self.running:
            return
        self._loop = loop
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"beacn-{self._topic}-consumer",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def join(self, timeout: float = 1.0) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                for envelope in self._client.subscribe(
                    self._topic,
                    last_event_id=self.last_event_id,
                ):
                    if self._stop.is_set():
                        return
                    self._persist(envelope)
                    self.last_event_id = envelope.id or self.last_event_id
                    self.last_error = None
                self._stop.wait(self._retry_delay)
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("BEACN consumer failed; retrying")
                self._stop.wait(self._retry_delay)

    def _persist(self, envelope: Any) -> None:
        try:
            event = EventModel.model_validate(envelope.data)
        except ValidationError as exc:
            self.last_error = f"invalid event {envelope.id}: {exc}"
            logger.warning("discarding invalid log event %s: %s", envelope.id, exc)
            return

        if self._loop is None or self._loop.is_closed():
            raise RuntimeError("event loop unavailable")
        metadata = {
            "id": envelope.id,
            "event": envelope.event,
            "topic": envelope.topic,
            "producer": envelope.producer,
            "environment": envelope.environment,
            "timestamp": envelope.timestamp,
            "severity": envelope.severity,
            "schema_version": envelope.schema_version,
            "seq": envelope.seq,
        }
        future = asyncio.run_coroutine_threadsafe(self._store.put(event, metadata), self._loop)
        try:
            future.result(timeout=self._write_timeout)
        except FutureTimeoutError:
            future.cancel()
            raise TimeoutError(f"Mongo write exceeded {self._write_timeout}s") from None
