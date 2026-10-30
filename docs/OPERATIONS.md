# Operations

## Production deployment

Build and run the supplied Docker image, or install with `uv sync --frozen` and
run `uv run uvicorn app.main:app --host 127.0.0.1 --port 8010` under your process
supervisor. Configure a persistent, access-controlled MongoDB deployment and a
BEACN consumer key. Set `LOGSMAN_DEBUG=false` explicitly. Use a TLS/authenticating
reverse proxy for remote API users: the application itself does not authenticate
reads. Avoid publishing an unauthenticated database or log archive publicly.

Start with one app worker. Each Uvicorn worker or app replica starts an
independent subscriber to the same topic; there is no consumer-group work
partitioning. Multiple replicas repeat Mongo writes. Payload-id uniqueness makes
normal duplicate delivery idempotent, but is not a scaling or delivery guarantee.

Startup pings MongoDB and creates these indexes:

- Unique payload `id`.
- `received_at` descending.
- `type`, `source` and `destination`, each paired with `received_at` descending.
- `tags` ascending.

Use credentials with the required collection/index permissions at startup.
Existing duplicate payload ids can prevent creation of the unique index. Review
and reconcile duplicates before retrying; do not blindly remove records.

## Verify and inspect

```sh
docker compose ps
docker compose logs --tail=100 logsman
curl --fail http://127.0.0.1:8010/api/health
```

Publish a known valid log through BEACN while Logsman is running, then retrieve
it by payload id from `/api/events/{id}`. This checks the actual credential,
network, topic environment, payload and storage path. Health alone cannot prove
that BEACN is connected or that events have arrived.

The listener runs in a daemon thread. Valid Mongo writes are scheduled onto the
app event loop and waited on for up to 30 seconds. Stream failures or write
failures are logged and the outer listener retries after five seconds. The
vendored SDK also has its own reconnect/backoff behavior. Shutdown requests the
thread to stop and waits briefly; a blocked stream read is not a durable drain.

## Retention, backups and recovery

Logsman has no built-in TTL, pruning job, deletion API, export endpoint or durable
consumer checkpoint. Plan MongoDB capacity and retention separately. Back up the
MongoDB data plus service configuration and protect backups as sensitive log data.
Test restoring into an isolated database before relying on backups.

The Compose `logsman-mongo` named volume survives ordinary container recreation.
`docker compose down -v` deletes it; do not use that command for routine updates.
For an update, keep a backup and use `docker compose up --build -d`.

The current consumer is a live SSE subscriber. Its last envelope id is in memory,
and the current BEACN SSE endpoint ignores the `Last-Event-ID` header sent by the
SDK. Neither process restarts nor connection retries guarantee recovery of
missed events. Invalid payloads are discarded with no dead-letter storage.
A replayed event with an already archived payload id keeps the first document.

If logs are missing, inspect the BEACN durable event history in the correct
environment, while it is still retained. An operator can build a controlled
backfill using BEACN's explicit replay API (`events:replay`) and the same payload
validation/store contract, or republish retained payloads while Logsman is live
using a publisher key. Backfill tooling is not bundled. Preserve payload ids so
archive writes remain idempotent, and monitor the result; republishing creates
new BEACN envelopes and can notify other consumers. BEACN's idempotency keys may
suppress republished events inside its deduplication window.

## Troubleshooting

| Symptom | Check / action |
| --- | --- |
| Startup says API key required | Set `LOGSMAN_BEACN_API_KEY`, or disable the listener intentionally |
| MongoDB startup/ping failure | URI, DNS, credentials, TLS, database availability and network access |
| Container cannot reach BEACN | Use a shared-network service name or reachable host address; container localhost is not the host |
| Host Python cannot resolve `mongo` | Use `mongodb://127.0.0.1:27017` for a host MongoDB instead of the Compose hostname |
| Health passes but no logs appear | Inspect listener exceptions; check key scopes/environment, topic and payload; run a publish/read smoke test |
| `discarding invalid log event` | Match `type`, required strings, tags and allowed fields in the payload contract |
| A second event did not change an archived record | Insert-once behavior by payload id; choose a fresh id for a distinct log |
| Missing events after downtime | Explicit BEACN history/replay/backfill; reconnect alone does not catch up |
| MongoDB grows without bound | Add an explicit retention/archival policy; no automatic TTL is configured |
| Docker Hub pulls time out | Compose uses `mirror.gcr.io/library/mongo:8`; retry the pull and inspect Docker/network status |

## Change the vendored SDK

`app/vendor/beacn.py` is part of this service's source and image. Installing a
new external `beacn` package does not replace it. Review and copy an intended SDK
revision, run the service tests, and test its stream behavior against the BEACN
server version being deployed before rolling out.
