# API and event contract

## Incoming log payload

Publish a BEACN envelope with any suitable event name (for example,
`log.created`), the configured topic (default `logs`), and this object in `data`:

```json
{
  "id": "log_123",
  "type": "error",
  "source": "checkout",
  "destination": "payments",
  "data": {"message": "card declined"},
  "tags": ["payments", "urgent"]
}
```

| Field | Required | Contract |
| --- | --- | --- |
| `id` | yes | String with at least one character; archive-wide unique key in this collection |
| `type` | yes | Exactly `error`, `info`, `debug` or `actions` |
| `source` | yes | String with at least one character |
| `destination` | yes | String with at least one character |
| `data` | no | Any JSON-compatible value; defaults to `{}` |
| `tags` | no | List of strings; defaults to `[]` |

Unknown top-level payload fields are forbidden. Strings are not trimmed, so
producers should reject whitespace-only identifiers themselves. Invalid payloads
are logged and discarded, not stored in a dead-letter queue. The listener then
continues to subsequent events. The payload type is independent of BEACN's
envelope severity or event name.

The archive stores the payload plus `received_at` (UTC ingestion time) and
`beacn` metadata: `id`, `event`, `topic`, `producer`, `environment`, `timestamp`,
`severity`, `schema_version` and `seq`. MongoDB's internal `_id` is excluded from
read responses. `received_at` is returned as an ISO-8601 UTC string.

Writes use `$setOnInsert` with an upsert on payload `id`. The first document wins,
even if a later event with that id has different content, a different source,
or another BEACN envelope id. Choose payload ids that do not collide across
publishers sharing a collection. Redelivery does not refresh `received_at`.

## Read endpoints

The API currently has no application authentication or write endpoints. Examples
assume a local trusted listener. Protect remote access with your gateway/proxy.
Generated API documentation is at `/docs`, with schema at `/openapi.json`.

### `GET /api/events`

Returns events ordered by `received_at` descending, then payload `id` descending.
All supplied filters are combined with AND.

| Query | Default | Meaning |
| --- | --- | --- |
| `type` | absent | Exact type; invalid values return 422 |
| `source` | absent | Exact source |
| `destination` | absent | Exact destination |
| `tag` | absent | Membership in the tags list, one tag |
| `limit` | `50` | Integer from 1 through 200 |
| `offset` | `0` | Non-negative integer |

```sh
curl 'http://127.0.0.1:8010/api/events?type=error&source=checkout&limit=20&offset=0'
```

```json
{
  "events": [],
  "count": 0,
  "limit": 20,
  "offset": 0
}
```

`count` is the number returned in this page, not a total count. An empty page is
HTTP 200. Offset pagination can shift while new events arrive; it is not a
stable export cursor. Invalid query bounds return validation errors.

### `GET /api/events/{event_id}`

Looks up the payload id, not the BEACN envelope id. HTTP 200 returns
`{"event": {...}}`; a missing id returns HTTP 404. URL-encode path values.

### `GET /api/health`

```json
{
  "status": "ok",
  "checks": {"mongodb": "ok", "beacn_listener": "ok"},
  "topic": "logs"
}
```

HTTP 200 means MongoDB responds to ping and the enabled listener thread is
alive. HTTP 503 means MongoDB is unavailable or the enabled listener thread is
stopped. If the listener is disabled, its check is reported as `ok`.

This check does not verify an authenticated/working BEACN subscription, recent
ingestion, or replay completeness. A thread retrying failed credentials can
still pass health. Observe logs and publish a known event to verify collection.
