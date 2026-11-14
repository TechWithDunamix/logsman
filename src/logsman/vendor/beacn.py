"""BEACN — official Python SDK.

One self-contained module. The synchronous client and the event-history and
consumer helpers use only the standard library (``urllib``), so ``pip install
beacn`` pulls in nothing. The asynchronous client and the WebSocket consumer use
two optional extras::

    pip install "beacn[async]"      # httpx        -> AsyncBeacn (REST)
    pip install "beacn[websocket]"  # websockets   -> WebSocket subscribe

Compatibility: Python 3.8+. The module deliberately avoids ``match``, the
``X | Y`` union syntax at runtime, ``:=`` in comprehensions and other syntax not
available on the oldest interpreter BEACN's own framework supports.

Quick start
-----------
    from beacn import Beacn

    beacn = Beacn(url="https://beacn.internal", api_key="bk_...")
    beacn.publish("payment.completed", topic="payments", data={"payment_id": "pay_1"})

    for event in beacn.subscribe("payments"):     # SSE, stdlib only
        print(event.event, event.data)

Async
----
    from beacn import AsyncBeacn

    beacn = AsyncBeacn(url="https://beacn.internal", api_key="bk_...")
    await beacn.publish("payment.completed", topic="payments", data={})
    async for event in beacn.subscribe("payments"):   # WebSocket
        print(event)
"""

from __future__ import annotations

import itertools
import json
import logging
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

__version__ = "0.1.0"
__all__ = [
    "Beacn",
    "AsyncBeacn",
    "Event",
    "PublishResult",
    "Page",
    "RetryPolicy",
    "BeacnError",
    "APIError",
    "AuthError",
    "RateLimitError",
    "ValidationError",
    "ConnectionClosed",
    "NotFound",
]

logger = logging.getLogger("beacn")

DEFAULT_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "beacn-python/" + __version__
_IDEMPOTENT_METHODS = ("GET", "HEAD", "PUT", "DELETE")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class BeacnError(Exception):
    """Base class for every error this SDK raises."""


class APIError(BeacnError):
    """The server answered with an error.

    Attributes:
        message: The server's error message (or ``HTTP <status>``).
        status: HTTP status code, when there was a response.
        code: BEACN error code such as ``invalid``, ``forbidden``, ``rate_limited``.
        details: Extra structured information from the server, if any.
        request_id: The ``X-Request-Id`` of the failed request, for support.
    """

    def __init__(self, message, *, status=None, code=None, details=None, request_id=None):
        # type: (str, Optional[int], Optional[str], Any, Optional[str]) -> None
        """Store the message and the response details."""
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code
        self.details = details
        self.request_id = request_id

    def __str__(self):  # type: () -> str
        """``[status] message (request_id=…)``, omitting the parts that are unknown."""
        base = self.message
        if self.status:
            base = "[%s] %s" % (self.status, base)
        if self.request_id:
            base = "%s (request_id=%s)" % (base, self.request_id)
        return base


class AuthError(APIError):
    """401/403: the API key is missing, invalid, revoked, or lacks a scope."""

    pass


class NotFound(APIError):
    """404: the event, task or other resource does not exist in this environment."""

    pass


class ValidationError(APIError):
    """422: the event or request was rejected as malformed."""

    pass


class RateLimitError(APIError):
    """429: the key exceeded its ingestion rate.

    Attributes:
        retry_after: Seconds to wait, from the ``Retry-After`` header.
    """

    def __init__(self, message, *, retry_after=1.0, **kw):
        # type: (str, float, Any) -> None
        """Store ``retry_after`` along with the usual error details."""
        super().__init__(message, **kw)
        self.retry_after = retry_after


class ConnectionClosed(BeacnError):
    """A realtime stream ended and could not be resumed."""

    pass


def _error_for(status, payload, request_id):
    # type: (int, Any, Optional[str]) -> APIError
    """Build the right :class:`APIError` subclass for an HTTP error response."""
    err = {}
    if isinstance(payload, dict):
        err = payload.get("error") or payload
    message = err.get("message") if isinstance(err, dict) else None
    message = message or ("HTTP %s" % status)
    code = err.get("code") if isinstance(err, dict) else None
    details = err.get("details") if isinstance(err, dict) else None
    kw = dict(status=status, code=code, details=details, request_id=request_id)
    if status in (401, 403):
        return AuthError(message, **kw)
    if status == 404:
        return NotFound(message, **kw)
    if status == 422:
        return ValidationError(message, **kw)
    if status == 429:
        retry_after = 1.0
        try:
            retry_after = float((details or {}).get("retry_after", 1.0))
        except (TypeError, ValueError, AttributeError):
            pass
        return RateLimitError(message, retry_after=retry_after, **kw)
    return APIError(message, **kw)


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


class RetryPolicy(object):
    """Exponential backoff with full jitter.

    Retries are attempted on connection errors, HTTP 429 and HTTP >= 500, and
    only for idempotent requests unless an ``idempotency_key`` was supplied
    (which makes a publish safe to repeat).
    """

    def __init__(
        self,
        max_attempts=4,
        base_delay=0.25,
        max_delay=20.0,
        max_elapsed=90.0,
        multiplier=2.0,
        jitter=True,
    ):
        # type: (int, float, float, float, float, bool) -> None
        """Configure retries.

        Args:
            max_attempts: Total attempts including the first (at least 1).
            base_delay: Delay before the first retry, in seconds.
            max_delay: Upper bound on any single delay.
            max_elapsed: Stop retrying once this many seconds have passed overall.
            multiplier: Exponential backoff factor between attempts.
            jitter: Randomise each delay ("full jitter") to avoid thundering herds.
        """
        self.max_attempts = max(1, max_attempts)
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_elapsed = max_elapsed
        self.multiplier = multiplier
        self.jitter = jitter

    def delay_for(self, attempt, retry_after=None):
        # type: (int, Optional[float]) -> float
        """Seconds to wait before attempt ``attempt + 1``.

        A server ``Retry-After`` value wins (capped at ``max_delay``); otherwise
        exponential backoff with optional jitter.
        """
        if retry_after is not None:
            return min(self.max_delay, max(0.0, retry_after))
        raw = self.base_delay * (self.multiplier ** (attempt - 1))
        raw = min(self.max_delay, raw)
        if self.jitter:
            return random.uniform(0.0, raw)
        return raw

    def should_retry(self, attempt, status, exc, idempotent):
        # type: (int, Optional[int], Optional[BaseException], bool) -> bool
        """Whether to retry after attempt number ``attempt``.

        Only idempotent requests are retried (publishes carry idempotency keys, so
        they count). Network errors, 429 and 5xx responses are retried; other 4xx
        responses are not.
        """
        if attempt >= self.max_attempts or not idempotent:
            return False
        if exc is not None:
            return True
        if status is None:
            return False
        return status == 429 or status >= 500


# ---------------------------------------------------------------------------
# Typed representations
# ---------------------------------------------------------------------------


class Event(object):
    """A BEACN event as delivered to a consumer or read from history."""

    __slots__ = ("id", "event", "topic", "kind", "producer", "environment",
                 "timestamp", "severity", "schema_version", "data", "seq", "raw")

    def __init__(self, payload):
        # type: (Dict[str, Any]) -> None
        """Wrap an event dict from the API or a realtime stream."""
        self.raw = payload
        self.id = payload.get("id")
        self.event = payload.get("event")
        self.topic = payload.get("topic")
        self.kind = payload.get("kind", "event")
        self.producer = payload.get("producer")
        self.environment = payload.get("environment")
        self.timestamp = payload.get("timestamp")
        self.severity = payload.get("severity", "info")
        self.schema_version = payload.get("schema_version", "1")
        self.data = payload.get("data", {}) or {}
        self.seq = payload.get("seq")

    @property
    def correlation_id(self):  # type: () -> Optional[str]
        """The event's correlation id, if it has one."""
        return self.raw.get("correlation_id")

    @property
    def is_task(self):  # type: () -> bool
        """Whether this is a ``task.*`` lifecycle event."""
        return self.kind == "task"

    def __repr__(self):  # type: () -> str
        """Short representation with id, name and topic."""
        return "Event(id=%r, event=%r, topic=%r)" % (self.id, self.event, self.topic)

    def __getitem__(self, key):  # type: (str) -> Any
        """Read any field of the raw event dict, e.g. ``event["severity"]``."""
        return self.raw[key]


class PublishResult(object):
    """What the server accepted for a publish call.

    Attributes:
        accepted: Number of events accepted (duplicates included).
        deduplicated: How many were duplicates of an earlier idempotency key.
        events: ``[{"id", "event", "topic", "deduplicated"}, ...]``.
        rejected: Events the server refused, with reasons (batch publishes).
        raw: The full response body.
    """

    __slots__ = ("accepted", "deduplicated", "events", "rejected", "raw")

    def __init__(self, payload):
        # type: (Dict[str, Any]) -> None
        """Parse the publish response body."""
        self.raw = payload
        self.accepted = payload.get("accepted", 0)
        self.deduplicated = payload.get("deduplicated", 0)
        self.events = payload.get("events", [])
        self.rejected = payload.get("rejected", [])

    @property
    def id(self):  # type: () -> Optional[str]
        """Id of the first accepted event (convenient for single publishes)."""
        return self.events[0]["id"] if self.events else None

    @property
    def ids(self):  # type: () -> List[str]
        """Ids of every accepted event, in request order."""
        return [e["id"] for e in self.events]

    def __repr__(self):  # type: () -> str
        """Short representation with the accepted and deduplicated counts."""
        return "PublishResult(accepted=%s, deduplicated=%s)" % (self.accepted, self.deduplicated)


class Page(object):
    """One page of event history. Iterate it, or call :meth:`next` for more."""

    def __init__(self, client, path, params, payload):
        # type: (Any, str, Dict[str, Any], Dict[str, Any]) -> None
        """Hold one page of history plus what is needed to fetch the next."""
        self._client = client
        self._path = path
        self._params = params
        self.events = [Event(e) for e in payload.get("events", [])]
        page = payload.get("page", {})
        self.next_cursor = page.get("next_cursor")
        self.has_more = bool(page.get("has_more"))

    def __iter__(self):  # type: () -> Iterator[Event]
        """Iterate the events on this page."""
        return iter(self.events)

    def __len__(self):  # type: () -> int
        """Number of events on this page."""
        return len(self.events)

    def next(self):  # type: () -> Optional["Page"]
        """Fetch the following page, or ``None`` when this is the last one."""
        if not self.has_more:
            return None
        params = dict(self._params)
        params["cursor"] = self.next_cursor
        return self._client._history_page(self._path, params)

    def autopage(self):  # type: () -> Iterator[Event]
        """Iterate every event across this and all following pages."""
        page = self  # type: Optional[Page]
        while page is not None:
            for event in page.events:
                yield event
            page = page.next()


# ---------------------------------------------------------------------------
# Shared config
# ---------------------------------------------------------------------------


class _Config(object):
    """Connection settings shared by :class:`Beacn` and :class:`AsyncBeacn`."""

    def __init__(self, url, api_key, timeout, retry, user_agent, default_topic,
                 default_source, environment):
        """Store settings, defaulting the retry policy and user agent."""
        self.base_url = url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.retry = retry or RetryPolicy()
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        self.default_topic = default_topic
        self.default_source = default_source
        self.environment = environment

    @property
    def is_ws(self):
        """Whether the configured URL uses ``ws://``/``wss://``."""
        return self.base_url.startswith("ws://") or self.base_url.startswith("wss://")

    def http_base(self):
        """The base URL as ``http(s)://`` (converting a ``ws(s)://`` URL)."""
        b = self.base_url
        if b.startswith("wss://"):
            return "https://" + b[6:]
        if b.startswith("ws://"):
            return "http://" + b[5:]
        return b

    def ws_base(self):
        """The base URL as ``ws(s)://`` (converting an ``http(s)://`` URL)."""
        b = self.base_url
        if b.startswith("https://"):
            return "wss://" + b[8:]
        if b.startswith("http://"):
            return "ws://" + b[7:]
        return b


def _build_body(event, topic, data, kw):
    # type: (str, Optional[str], Optional[Dict[str, Any]], Dict[str, Any]) -> Dict[str, Any]
    """The JSON body for one event: ``event``, ``topic``, ``data`` and any extra envelope fields."""
    body = {"event": event}
    if topic:
        body["topic"] = topic
    if data is not None:
        body["data"] = data
    for field in ("source", "occurred_at", "correlation_id", "request_id", "user_id",
                  "organization_id", "project_id", "schema_version", "severity",
                  "idempotency_key"):
        if field in kw and kw[field] is not None:
            body[field] = kw[field]
    return body


# ---------------------------------------------------------------------------
# Synchronous client (standard library only)
# ---------------------------------------------------------------------------


class Beacn(object):
    """Synchronous BEACN client (standard library only).

    Args:
        url: BEACN base URL, e.g. ``https://beacn.internal``.
        api_key: A ``bk_…`` key. Its scopes decide what the client may do.
        timeout: Per-request timeout in seconds.
        retry: A :class:`RetryPolicy`; the default retries idempotent calls.
        user_agent: Override the ``User-Agent`` header.
        default_topic: Topic used when ``publish`` is called without one.
        default_source: ``source`` stamped on events that do not set one.
        environment: Informational; the key decides the environment server-side.

    Use as a context manager or call :meth:`close` when done.
    """

    def __init__(
        self,
        url,
        api_key,
        timeout=DEFAULT_TIMEOUT,
        retry=None,
        user_agent=None,
        default_topic=None,
        default_source=None,
        environment=None,
    ):
        # type: (str, str, float, Optional[RetryPolicy], Optional[str], Optional[str], Optional[str], Optional[str]) -> None
        """Validate the key and prepare the HTTP opener.

        Raises:
            BeacnError: ``api_key`` is empty.
        """
        if not api_key:
            raise BeacnError("api_key is required")
        self._cfg = _Config(url, api_key, timeout, retry, user_agent,
                            default_topic, default_source, environment)
        self._opener = urllib.request.build_opener()

    # -- low-level request --------------------------------------------

    def _request(self, method, path, body=None, params=None, idempotent=None):
        # type: (str, str, Any, Optional[Dict[str, Any]], Optional[bool]) -> Any
        """Send one API request with retries and return the decoded JSON body.

        Raises:
            APIError: The final attempt failed (see the subclasses for which kind).
        """
        url = self._cfg.http_base() + path
        if params:
            url = url + "?" + urllib.parse.urlencode(
                {k: v for k, v in params.items() if v is not None}
            )
        payload = None
        if body is not None:
            payload = json.dumps(body).encode("utf-8")

        if idempotent is None:
            idempotent = method in _IDEMPOTENT_METHODS or (
                isinstance(body, dict) and bool(body.get("idempotency_key"))
            ) or (isinstance(body, dict) and "events" in body and all(
                isinstance(e, dict) and e.get("idempotency_key") for e in body["events"]
            ))

        retry = self._cfg.retry
        start = time.time()
        attempt = 0
        while True:
            attempt += 1
            request_id = "req_" + uuid.uuid4().hex[:24]
            req = urllib.request.Request(url, data=payload, method=method)
            req.add_header("Authorization", "Bearer " + self._cfg.api_key)
            req.add_header("User-Agent", self._cfg.user_agent)
            req.add_header("X-Request-Id", request_id)
            if payload is not None:
                req.add_header("Content-Type", "application/json")
            status = None
            exc = None
            try:
                resp = self._opener.open(req, timeout=self._cfg.timeout)
                raw = resp.read()
                return _decode(raw, resp.headers.get("Content-Type"))
            except urllib.error.HTTPError as e:  # noqa: PERF203
                status = e.code
                raw = e.read()
                parsed = _decode_safe(raw)
                api_exc = _error_for(status, parsed, e.headers.get("X-Request-Id") or request_id)
                if retry.should_retry(attempt, status, None, idempotent):
                    delay = retry.delay_for(
                        attempt, getattr(api_exc, "retry_after", None)
                    )
                    if time.time() - start + delay <= retry.max_elapsed:
                        logger.debug("retrying %s %s in %.2fs (status %s)", method, path, delay, status)
                        time.sleep(delay)
                        continue
                raise api_exc
            except (urllib.error.URLError, OSError) as e:
                exc = e
                if retry.should_retry(attempt, None, e, idempotent):
                    delay = retry.delay_for(attempt)
                    if time.time() - start + delay <= retry.max_elapsed:
                        logger.debug("retrying %s %s in %.2fs (%s)", method, path, delay, e)
                        time.sleep(delay)
                        continue
                raise APIError("request failed: %s" % exc, request_id=request_id)

    # -- publishing --------------------------------------------------

    def publish(self, event, topic=None, data=None, **kw):
        # type: (str, Optional[str], Optional[Dict[str, Any]], Any) -> PublishResult
        """Publish one event.

        Args:
            event: Event name, e.g. ``"payment.completed"``.
            topic: Topic; defaults to ``default_topic``.
            data: JSON-serialisable payload.
            **kw: Other envelope fields: ``idempotency_key``, ``correlation_id``,
                ``severity``, ``source``, ``user_id``, ``occurred_at`` and so on.

        Returns:
            A :class:`PublishResult`.
        """
        body = _build_body(event, topic or self._cfg.default_topic, data, kw)
        if "source" not in body and self._cfg.default_source:
            body["source"] = self._cfg.default_source
        return PublishResult(self._request("POST", "/api/v1/events", body=body))

    def publish_batch(self, events, idempotency_key=None):
        # type: (Sequence[Dict[str, Any]], Optional[str]) -> PublishResult
        """Publish several events in one request.

        With ``idempotency_key`` each event without its own key gets
        ``"<key>:<index>"``, so retrying the whole batch is safe.
        """
        items = []
        for e in events:
            item = dict(e)
            if "topic" not in item and self._cfg.default_topic:
                item["topic"] = self._cfg.default_topic
            items.append(item)
        body = {"events": items}
        if idempotency_key:
            for i, item in enumerate(body["events"]):
                item.setdefault("idempotency_key", "%s:%d" % (idempotency_key, i))
        return PublishResult(self._request("POST", "/api/v1/events", body=body))

    # -- history ---------------------------------------------------

    def history(self, topic=None, event=None, event_prefix=None, kind=None,
                severity=None, correlation_id=None, since=None, until=None,
                order="desc", limit=50, cursor=None):
        # type: (...) -> Page
        """Query stored events (newest first by default); returns the first :class:`Page`."""
        params = {
            "topic": topic, "event": event, "event_prefix": event_prefix,
            "kind": kind, "severity": severity, "correlation_id": correlation_id,
            "since": since, "until": until, "order": order, "limit": limit, "cursor": cursor,
        }
        return self._history_page("/api/v1/events", params)

    def _history_page(self, path, params):
        """Fetch one history page for the given parameters."""
        payload = self._request("GET", path, params=params)
        return Page(self, path, params, payload)

    def get_event(self, event_id):
        # type: (str) -> Event
        """Fetch one event by id.

        Raises:
            NotFound: No such event in this key's environment.
        """
        return Event(self._request("GET", "/api/v1/events/" + event_id)["event"])

    def iter_events(self, **kw):
        # type: (Any) -> Iterator[Event]
        """Iterate every matching event across pages (same filters as :meth:`history`)."""
        return self.history(**kw).autopage()

    # -- replay --------------------------------------------------

    def replay(self, topic, since=None, limit=None):
        # type: (str, Optional[str], Optional[int]) -> Dict[str, Any]
        """Stored events on ``topic`` after the event id ``since`` (the server's replay response
        dict).
        """
        body = {"topic": topic}
        if since is not None:
            body["since"] = since
        if limit is not None:
            body["limit"] = limit
        return self._request("POST", "/api/v1/events/replay", body=body, idempotent=True)

    # -- consumer (SSE, stdlib) --------------------------------

    def subscribe(self, *topics, **kw):
        # type: (str, Any) -> Iterator[Event]
        """Yield events from one or more topics over SSE.

        Reconnects automatically with backoff, resuming from the last event id
        seen so no persisted event between the drop and the reconnect is missed
        (bounded by the server's retention).
        """
        reconnect = kw.pop("reconnect", True)
        token = kw.pop("token", None) or self._realtime_token(topics)
        last_id = kw.pop("last_event_id", None)
        retry = RetryPolicy(max_attempts=10 ** 9, base_delay=0.5, max_delay=30.0,
                            max_elapsed=10 ** 9)
        attempt = 0
        while True:
            try:
                for event in self._sse_once(topics, token, last_id):
                    last_id = event.id or last_id
                    attempt = 0
                    yield event
                return  # server closed cleanly
            except (urllib.error.URLError, OSError, ConnectionClosed) as e:
                if not reconnect:
                    raise ConnectionClosed(str(e))
                attempt += 1
                delay = retry.delay_for(attempt)
                logger.info("SSE reconnect in %.1fs (%s)", delay, e)
                time.sleep(delay)
                # refresh a possibly-expired token
                try:
                    token = self._realtime_token(topics)
                except APIError:
                    pass

    def _sse_once(self, topics, token, last_id):
        """Open one SSE request for ``topics`` (resuming after ``last_id`` when given)."""
        params = {"token": token, "topics": ",".join(topics)}
        url = self._cfg.http_base() + "/api/v1/sse?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url)
        req.add_header("Accept", "text/event-stream")
        if last_id:
            req.add_header("Last-Event-ID", last_id)
        resp = self._opener.open(req, timeout=None)
        for payload in _iter_sse(resp):
            if isinstance(payload, dict) and payload.get("type") == "event":
                yield Event(payload)

    def _realtime_token(self, topics):
        # type: (Iterable[str]) -> str
        """Mint a realtime token covering ``topics`` with this client's API key."""
        body = {"topic_patterns": list(topics)}
        out = self._request("POST", "/api/v1/realtime/tokens", body=body, idempotent=True)
        return out["token"]

    # -- misc ------------------------------------------------

    def whoami(self):  # type: () -> Dict[str, Any]
        """The producer, environment and scopes behind the API key."""
        return self._request("GET", "/api/v1/whoami")

    def close(self):  # type: () -> None
        """Release resources (nothing to release for the stdlib client; kept for symmetry)."""
        pass

    def __enter__(self):  # type: () -> "Beacn"
        """Use the client as a context manager."""
        return self

    def __exit__(self, *exc):  # type: (Any) -> None
        """Close the client on leaving the ``with`` block."""
        self.close()


# ---------------------------------------------------------------------------
# Asynchronous client (optional extras)
# ---------------------------------------------------------------------------


class AsyncBeacn(object):
    """Asynchronous BEACN client (needs ``httpx``; WebSocket subscribe needs ``websockets``).

    Takes the same arguments as :class:`Beacn`. Use ``async with`` or call
    :meth:`aclose` to close the underlying HTTP connection pool.
    """

    def __init__(
        self,
        url,
        api_key,
        timeout=DEFAULT_TIMEOUT,
        retry=None,
        user_agent=None,
        default_topic=None,
        default_source=None,
        environment=None,
    ):
        """Validate the key; the HTTP client is created lazily on first request.

        Raises:
            BeacnError: ``api_key`` is empty.
        """
        if not api_key:
            raise BeacnError("api_key is required")
        self._cfg = _Config(url, api_key, timeout, retry, user_agent,
                            default_topic, default_source, environment)
        self._client = None

    def _http(self):
        """The shared ``httpx.AsyncClient``, created on first use.

        Raises:
            BeacnError: ``httpx`` is not installed (``pip install "beacn[async]"``).
        """
        if self._client is None:
            try:
                import httpx
            except ImportError:
                raise BeacnError(
                    "AsyncBeacn needs httpx: pip install 'beacn[async]'"
                )
            self._client = httpx.AsyncClient(
                base_url=self._cfg.http_base(),
                timeout=self._cfg.timeout,
                headers={
                    "Authorization": "Bearer " + self._cfg.api_key,
                    "User-Agent": self._cfg.user_agent,
                },
            )
        return self._client

    async def _request(self, method, path, body=None, params=None, idempotent=None):
        """Async version of :meth:`Beacn._request` (same retry rules)."""
        import asyncio

        client = self._http()
        if idempotent is None:
            idempotent = method in _IDEMPOTENT_METHODS or (
                isinstance(body, dict) and bool(body.get("idempotency_key"))
            )
        retry = self._cfg.retry
        start = time.time()
        attempt = 0
        while True:
            attempt += 1
            request_id = "req_" + uuid.uuid4().hex[:24]
            try:
                resp = await client.request(
                    method, path, json=body, params=_clean(params),
                    headers={"X-Request-Id": request_id},
                )
            except Exception as e:  # httpx.TransportError and friends
                if retry.should_retry(attempt, None, e, idempotent):
                    delay = retry.delay_for(attempt)
                    if time.time() - start + delay <= retry.max_elapsed:
                        await asyncio.sleep(delay)
                        continue
                raise APIError("request failed: %s" % e, request_id=request_id)
            if resp.status_code >= 400:
                parsed = _decode_safe(resp.content)
                api_exc = _error_for(
                    resp.status_code, parsed,
                    resp.headers.get("x-request-id") or request_id,
                )
                if retry.should_retry(attempt, resp.status_code, None, idempotent):
                    delay = retry.delay_for(attempt, getattr(api_exc, "retry_after", None))
                    if time.time() - start + delay <= retry.max_elapsed:
                        await asyncio.sleep(delay)
                        continue
                raise api_exc
            return _decode(resp.content, resp.headers.get("content-type"))

    async def publish(self, event, topic=None, data=None, **kw):
        """Publish one event; see :meth:`Beacn.publish`."""
        body = _build_body(event, topic or self._cfg.default_topic, data, kw)
        if "source" not in body and self._cfg.default_source:
            body["source"] = self._cfg.default_source
        return PublishResult(await self._request("POST", "/api/v1/events", body=body))

    async def publish_batch(self, events, idempotency_key=None):
        """Publish several events; see :meth:`Beacn.publish_batch`."""
        items = [dict(e) for e in events]
        body = {"events": items}
        if idempotency_key:
            for i, item in enumerate(items):
                item.setdefault("idempotency_key", "%s:%d" % (idempotency_key, i))
        return PublishResult(await self._request("POST", "/api/v1/events", body=body))

    async def history(self, **kw):
        """Query stored events; returns the response dict (``events`` and ``page``)."""
        params = {k: kw.get(k) for k in (
            "topic", "event", "event_prefix", "kind", "severity", "correlation_id",
            "since", "until", "order", "limit", "cursor")}
        params.setdefault("order", "desc")
        params["limit"] = kw.get("limit", 50)
        payload = await self._request("GET", "/api/v1/events", params=params)
        return [Event(e) for e in payload.get("events", [])], payload.get("page", {})

    async def get_event(self, event_id):
        """Fetch one event by id."""
        out = await self._request("GET", "/api/v1/events/" + event_id)
        return Event(out["event"])

    async def replay(self, topic, since=None, limit=None):
        """Stored events on ``topic`` after ``since``; see :meth:`Beacn.replay`."""
        body = {"topic": topic}
        if since is not None:
            body["since"] = since
        if limit is not None:
            body["limit"] = limit
        return await self._request("POST", "/api/v1/events/replay", body=body, idempotent=True)

    async def whoami(self):
        """The producer, environment and scopes behind the API key."""
        return await self._request("GET", "/api/v1/whoami")

    async def _realtime_token(self, topics):
        """Mint a realtime token covering ``topics``."""
        out = await self._request(
            "POST", "/api/v1/realtime/tokens", body={"topic_patterns": list(topics)},
            idempotent=True,
        )
        return out["token"]

    async def subscribe(self, *topics, **kw):
        # type: (str, Any) -> AsyncIterator[Event]
        """Async-iterate events from `topics` over a WebSocket, with reconnect
        and replay-on-reconnect from the last seen id."""
        try:
            import websockets
        except ImportError:
            raise BeacnError(
                "AsyncBeacn.subscribe needs websockets: pip install 'beacn[websocket]'"
            )
        import asyncio

        reconnect = kw.pop("reconnect", True)
        heartbeat = kw.pop("heartbeat", True)
        last_id = kw.pop("last_event_id", None)
        backoff = itertools.chain([0.5, 1, 2, 5, 10], itertools.repeat(15))
        while True:
            token = await self._realtime_token(topics)
            ws_url = self._cfg.ws_base() + "/realtime?token=" + urllib.parse.quote(token)
            try:
                async with websockets.connect(ws_url, open_timeout=self._cfg.timeout) as ws:
                    backoff = itertools.chain([0.5, 1, 2, 5, 10], itertools.repeat(15))
                    await ws.recv()  # welcome
                    for topic in topics:
                        await ws.send(json.dumps({"type": "subscribe", "topic": topic}))
                    if last_id:
                        for topic in topics:
                            await ws.send(json.dumps(
                                {"type": "replay", "topic": topic, "since": last_id}
                            ))
                    async for raw in _ws_iter(ws, heartbeat):
                        frame = json.loads(raw)
                        ftype = frame.get("type")
                        if ftype == "event":
                            ev = Event(frame)
                            last_id = ev.id or last_id
                            yield ev
                        elif ftype == "error":
                            raise APIError(frame.get("message", "realtime error"),
                                           code=frame.get("code"))
            except (OSError, ConnectionClosed) as e:
                if not reconnect:
                    raise ConnectionClosed(str(e))
                await asyncio.sleep(next(backoff))
            except Exception as e:  # websockets.ConnectionClosed etc.
                if "ConnectionClosed" not in type(e).__name__ or not reconnect:
                    raise
                await asyncio.sleep(next(backoff))

    async def aclose(self):
        """Close the HTTP connection pool."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        """Use the client as an async context manager."""
        return self

    async def __aexit__(self, *exc):
        """Close the client on leaving the ``async with`` block."""
        await self.aclose()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean(params):
    """Drop ``None`` values from query parameters (``None`` if nothing is left)."""
    if not params:
        return None
    return {k: v for k, v in params.items() if v is not None}


def _decode(raw, content_type):
    """Decode a response body: JSON normally, text for ``text/plain``, ``{}`` when empty."""
    if not raw:
        return {}
    if content_type and "json" not in content_type and "text/plain" in content_type:
        return raw.decode("utf-8")
    return json.loads(raw.decode("utf-8"))


def _decode_safe(raw):
    """Decode JSON, returning ``{}`` for anything unparsable."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, AttributeError):
        return {}


def _iter_sse(resp):
    # type: (Any) -> Iterator[Any]
    """Parse a text/event-stream response body into decoded ``data:`` payloads."""
    buf = []
    for line_bytes in resp:
        line = line_bytes.decode("utf-8").rstrip("\n").rstrip("\r")
        if line == "":
            if buf:
                data = "\n".join(buf)
                buf = []
                try:
                    yield json.loads(data)
                except ValueError:
                    yield data
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            buf.append(line[5:].lstrip(" "))


async def _ws_iter(ws, heartbeat):
    # type: (Any, bool) -> AsyncIterator[str]
    """Yield raw WebSocket messages, answering server heartbeats when ``heartbeat`` is on."""
    import asyncio

    if not heartbeat:
        async for raw in ws:
            yield raw
        return
    while True:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=20.0)
        except asyncio.TimeoutError:
            await ws.send(json.dumps({"type": "ping"}))
            continue
        yield raw
