# Logsman

Logsman archives structured log events from BEACN into MongoDB and exposes a
read-only HTTP API. It runs on Sillo with a background SSE consumer and an async
MongoDB client. The BEACN Python SDK is vendored in `app/vendor/beacn.py`; a
neighboring BEACN checkout is not required at runtime.

```text
Application → BEACN topic `logs` → Logsman listener → MongoDB
                                                      ↓
                                              Logsman read API
```

| Guide | Contents |
| --- | --- |
| [Configuration](docs/CONFIGURATION.md) | Every environment setting, local and container networking |
| [API and event contract](docs/API.md) | Publish payload, storage semantics, filters and responses |
| [Operations](docs/OPERATIONS.md) | Deployment, health, backup, recovery and troubleshooting |

## Requirements

- Python 3.11+ for local development; the Docker image uses Python 3.12.
- MongoDB reachable by Logsman; Compose supplies MongoDB 8.
- A running BEACN server and an active API key with `events:read` in the same
  isolation environment as your log publishers.
- `uv` for local dependency installation, or Docker with Compose for containers.

## Run with Docker Compose

```sh
cp .env.example .env
# Set LOGSMAN_BEACN_API_KEY to a real BEACN consumer key.
# Review LOGSMAN_BEACN_URL for your network topology.
docker compose up --build -d
curl --fail http://127.0.0.1:8010/api/health
```

The example environment points MongoDB to the Compose service `mongo` and BEACN
to the Docker host at `http://host.docker.internal:8000`. On Linux, a BEACN
listener bound only to host loopback may not be reachable through the bridge;
use a reachable, access-controlled host address or a shared Docker network.
If BEACN is another container on a shared network, use its service name instead.

Compose persists MongoDB in the `logsman-mongo` volume and does not publish a
MongoDB host port. The HTTP API is published on port 8010. The bundled MongoDB
has no authentication and the read API has no authentication middleware; keep
the deployment on a trusted network or place an authenticated proxy in front.
Use the [operations guide](docs/OPERATIONS.md) before production.

## Run locally without Docker

```sh
cp .env.example .env
# Change these .env values for services running on the host:
# LOGSMAN_MONGODB_URL=mongodb://127.0.0.1:27017
# LOGSMAN_BEACN_URL=http://127.0.0.1:8000
# LOGSMAN_BEACN_API_KEY=<your consumer key>
uv sync --frozen --extra dev
uv run uvicorn app.main:app --reload --host 127.0.0.1 --port 8010
```

Start MongoDB and BEACN separately. Startup pings MongoDB and creates indexes;
a MongoDB connection/index failure prevents the app from starting. The API key
must be nonempty if the consumer is enabled; a rejected key causes listener
connection failures rather than a MongoDB startup failure.

To serve only the HTTP API, set `LOGSMAN_BEACN_ENABLED=false`. This disables
collection of new events; it is useful during maintenance or read-only use.

## Send your first log

Use a BEACN publisher key with `events:publish` in the consumer's environment.
This key is separate from Logsman's consumer key:

```sh
export BEACN_URL='http://127.0.0.1:8000'
export BEACN_PUBLISH_KEY='bk_REPLACE'
curl --fail-with-body "$BEACN_URL/api/v1/events" \
  -H "Authorization: Bearer $BEACN_PUBLISH_KEY" \
  -H 'Content-Type: application/json' \
  --data '{"event":"log.created","topic":"logs","idempotency_key":"log_123","data":{"id":"log_123","type":"error","source":"checkout","destination":"payments","data":{"message":"card declined","order_id":"ord_123"},"tags":["payments","urgent"]}}'

curl --fail 'http://127.0.0.1:8010/api/events?type=error&tag=urgent&limit=20'
curl --fail http://127.0.0.1:8010/api/events/log_123
```

The log object belongs inside the BEACN envelope's `data`. Its `id` is the
archive's unique key; it is distinct from BEACN's generated `evt_…` envelope id.
`type` accepts only `error`, `info`, `debug` or `actions`. See the
[full contract](docs/API.md) for defaults and validation.

## Delivery limits

MongoDB uses insert-once upserts keyed by payload `id`: redelivery does not
replace the first stored document. The listener reconnects after stream errors,
but its checkpoint exists only in memory. The current BEACN SSE route does not
honor `Last-Event-ID`; Logsman does not perform explicit replay. Events published
while disconnected can therefore be absent from the archive. This is not a
lossless or exactly-once logging pipeline. See [recovery](docs/OPERATIONS.md).

## Develop and test

```sh
uv sync --frozen --extra dev
uv run pytest
```

Tests use injected in-memory storage and fake listeners; they do not require live
MongoDB or BEACN. They cover payload validation, read/filter endpoints, health,
and consumer persistence behavior. A live end-to-end smoke test is still needed
for a deployment's networking and credentials.

## Project layout

| Path | Purpose |
| --- | --- |
| `app/config.py` | Typed `LOGSMAN_` configuration |
| `app/bootstrap.py` | Application construction and startup/shutdown |
| `app/listener.py` | Background BEACN SSE consumer |
| `app/models.py` | Strict log payload validation |
| `app/store.py` | Async MongoDB persistence, indexes and serialization |
| `app/vendor/beacn.py` | Bundled BEACN client |
| `routes/api.py` | Events and health endpoints |
| `tests/` | Isolated service tests |
