"""Background BEACN consumer.

The bundled BEACN SDK's SSE subscription is synchronous (stdlib ``urllib``), so
the consumer runs in its own daemon thread and hands each write back to the
application's asyncio event loop with :func:`asyncio.run_coroutine_threadsafe`.
That keeps one MongoDB client on one loop while the blocking HTTP stream never
stalls request handling.

Delivery semantics (see the README's *Delivery limits*):

* at-most-once from Logsman's side — the resume checkpoint lives only in memory
  and BEACN's SSE route does not replay from ``Last-Event-ID``, so events
  published while the consumer is disconnected may be missing;
* duplicates are harmless — the store keeps the first copy per log ``id``.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any

from pydantic import ValidationError

from logsman.events.schema import EventModel
from logsman.storage.base import EventStore
from logsman.vendor.beacn import Beacn

logger = logging.getLogger("logsman.listener")


class BeacnLogListener:
    """Consume one BEACN topic in a daemon thread and persist valid payloads.

    The thread loops forever until :meth:`stop`: it opens an SSE subscription,
    validates and stores each event, and on any error (network drop, BEACN
    restart, rejected key, MongoDB timeout) records the error, waits
    ``retry_delay`` seconds and reconnects.

    Args:
        url: BEACN base URL, e.g. ``http://127.0.0.1:8000``.
        api_key: BEACN key with ``events:read`` in the publishers' environment.
        topic: Topic to subscribe to.
        store: Where valid logs are written.
        retry_delay: Seconds to wait before reconnecting after the stream ends
            or fails. Also bounds how quickly :meth:`stop` takes effect while
            waiting.
        write_timeout: Seconds to wait for one store write before treating it
            as failed (the stream is then reconnected).

    Attributes:
        last_event_id: Envelope id of the last event processed; passed back to
            the SDK on reconnect.
        last_error: Text of the most recent failure, or ``None`` after a
            successful event. Useful when debugging a silent archive.
    """

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
        """Create the SDK client and stop event; the thread starts in :meth:`start`."""
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
        """Whether the consumer thread is alive. Reported by ``GET /api/health``."""
        return self._thread is not None and self._thread.is_alive()

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start the consumer thread; a no-op if it is already running.

        Args:
            loop: The application's running event loop. Store writes are
                scheduled on it, so it must outlive the thread.
        """
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
        """Ask the thread to exit at its next check. Does not block."""
        self._stop.set()

    def join(self, timeout: float = 1.0) -> None:
        """Wait up to ``timeout`` seconds for the thread to finish.

        A thread blocked inside an idle SSE read may outlive the timeout; it is
        a daemon thread, so it never keeps the process alive.
        """
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        """Thread body: subscribe, persist, and reconnect until stopped."""
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
        """Validate one envelope's ``data`` and write it through the event loop.

        Invalid payloads are logged, recorded in :attr:`last_error` and skipped
        — they never stop the stream. A write that cannot be scheduled or does
        not finish within ``write_timeout`` raises, which makes :meth:`_run`
        reconnect.

        Raises:
            RuntimeError: The application's event loop is gone.
            TimeoutError: The store write took longer than ``write_timeout``.
        """
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
