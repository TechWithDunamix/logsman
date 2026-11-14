# Logsman

Logsman archives structured log events from [BEACN](https://github.com/TechWithDunamix/beacn)
into MongoDB and serves them through a small read-only HTTP API. It runs on the
[Sillo](https://pypi.org/project/sillo-framework/) web framework with a
background BEACN consumer and PyMongo's native async client.

```text
your services ──publish──► BEACN topic `logs` ──SSE──► Logsman consumer ──► MongoDB
                                                                              │
                                     GET /api/events, /api/events/{id} ◄──────┘
```

The BEACN Python SDK is vendored in `src/logsman/vendor/beacn.py`, so Logsman
installs from PyPI packages alone and never needs a BEACN checkout.

| Guide | Contents |
| --- | --- |
| [Configuration](docs/CONFIGURATION.md) | Every setting, local and container networking |
| [API and event contract](docs/API.md) | Publish payload, storage semantics, filters and responses |
| [Operations](docs/OPERATIONS.md) | Health, backup, recovery and troubleshooting |

---

## Project layout

```
logsman/
├── src/logsman/
│   ├── main.py            ASGI entry point: uvicorn logsman.main:app
│   ├── __main__.py        `logsman` / `python -m logsman` (runs uvicorn)
│   ├── core/              settings (config.py) and app assembly (app.py)
│   ├── events/            the strict log payload contract (schema.py)
│   ├── storage/           store protocol, MongoDB store, in-memory store
│   ├── consumer/          background BEACN SSE consumer thread
│   ├── api/               HTTP routes: events and health
│   └── vendor/            bundled BEACN Python SDK
├── tests/                 isolated tests (in-memory store, no network)
├── deploy/                systemd unit and production env template
├── docs/                  configuration, API and operations guides
├── requirements.txt       pinned runtime dependencies + this project
├── requirements-dev.txt   runtime + test/lint tools
├── uv.lock                lock file the requirements files are exported from
└── pyproject.toml, Dockerfile, docker-compose.yml, .env.example
```

Every module, class and function has a docstring; `src/logsman/core/app.py`
shows how the pieces are wired together.

---

## Requirements

- Python 3.11 or newer.
- MongoDB 6 or newer (Docker Compose supplies MongoDB 8).
- A running BEACN server and an API key with the `events:read` scope, in the same
  BEACN environment (`development`/`staging`/`production`) as the services that
  publish logs.

---

## Local setup

```bash
git clone https://github.com/TechWithDunamix/logsman.git && cd logsman
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt       # pinned deps + Logsman itself (editable)

cp .env.example .env
#   For services on this machine, change in .env:
#   LOGSMAN_MONGODB_URL=mongodb://127.0.0.1:27017
#   LOGSMAN_BEACN_URL=http://127.0.0.1:8000
#   LOGSMAN_BEACN_API_KEY=<a BEACN key with events:read>

logsman --reload                          # or: uvicorn logsman.main:app --reload --port 8010
curl --fail http://127.0.0.1:8010/api/health
```

About the requirements files:

- `requirements.txt` pins every runtime dependency (sillo-framework, pymongo,
  uvicorn and their dependencies) and ends with `-e .`, which installs Logsman
  itself so `logsman.main:app` and the `logsman` command work from any
  directory. Run it from the project root.
- `requirements-dev.txt` adds pytest, pytest-asyncio and ruff.
- Both are exported from `uv.lock`. After changing `pyproject.toml`, run
  `uv lock` and then
  `uv export --frozen --no-hashes --no-dev --format requirements-txt -o requirements.txt`
  (add `--extra dev` and `-o requirements-dev.txt` for the dev file).
- With [uv](https://docs.astral.sh/uv/) you can skip pip entirely:
  `uv sync --frozen --extra dev` and `uv run logsman --reload`.

Startup pings MongoDB and creates indexes; if MongoDB is unreachable the app
refuses to start. To serve only the read API (no consumer), set
`LOGSMAN_BEACN_ENABLED=false`.

---

## Configuration

Settings come from environment variables with the `LOGSMAN_` prefix, falling
back to `.env` in the working directory, then to defaults. Full table in
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

| Variable | Default | Purpose |
| --- | --- | --- |
| `LOGSMAN_APP_ENV` | `local` | Label: `local`, `testing`, `staging`, `production` |
| `LOGSMAN_DEBUG` | `true` | Detailed error pages. **Set `false` in production** |
| `LOGSMAN_HOST` / `LOGSMAN_PORT` | `127.0.0.1` / `8010` | Bind address for the `logsman` command |
| `LOGSMAN_MONGODB_URL` | `mongodb://localhost:27017` | MongoDB connection string |
| `LOGSMAN_MONGODB_DATABASE` / `_COLLECTION` | `logsman` / `events` | Where logs are stored |
| `LOGSMAN_BEACN_URL` | `http://localhost:8000` | BEACN base URL |
| `LOGSMAN_BEACN_API_KEY` | (empty) | BEACN key with `events:read`; required when the consumer is enabled |
| `LOGSMAN_BEACN_TOPIC` | `logs` | Topic to consume |
| `LOGSMAN_BEACN_ENABLED` | `true` | Run the consumer; `false` serves the read API only |

---

## Send your first log

Publish with a BEACN key that has `events:publish` (separate from Logsman's
consumer key) in the same environment:

```bash
export BEACN_URL='http://127.0.0.1:8000'
export BEACN_PUBLISH_KEY='bk_REPLACE'
curl --fail-with-body "$BEACN_URL/api/v1/events" \
  -H "Authorization: Bearer $BEACN_PUBLISH_KEY" \
  -H 'Content-Type: application/json' \
  --data '{"event":"log.created","topic":"logs","idempotency_key":"log_123","data":{"id":"log_123","type":"error","source":"checkout","destination":"payments","data":{"message":"card declined","order_id":"ord_123"},"tags":["payments","urgent"]}}'

curl --fail 'http://127.0.0.1:8010/api/events?type=error&tag=urgent&limit=20'
curl --fail http://127.0.0.1:8010/api/events/log_123
```

The log object goes in the BEACN event's `data`. Its `id` is the archive key
(distinct from BEACN's `evt_…` id); `type` must be `error`, `info`, `debug` or
`actions`; unknown fields are rejected. Invalid payloads are skipped and logged.
Full contract: [docs/API.md](docs/API.md).

## HTTP API

| Endpoint | Purpose |
| --- | --- |
| `GET /api/events` | List logs, newest first. Filters: `type`, `source`, `destination`, `tag`; paging: `limit` (1–200, default 50), `offset` |
| `GET /api/events/{id}` | One log by its payload `id` (404 if absent) |
| `GET /api/health` | MongoDB and consumer status: 200 `ok` or 503 `degraded` |
| `GET /openapi.json` | OpenAPI document |

The API has **no authentication**. Keep it on a private network or behind an
authenticating reverse proxy.

---

## Production without Docker

A manual install on Debian/Ubuntu using `deploy/systemd/logsman.service`. The
unit expects the code in `/opt/logsman`, a `logsman` system user and settings
in `/etc/logsman/logsman.env`.

### 1. MongoDB with authentication

Install MongoDB from the [official repository](https://www.mongodb.com/docs/manual/administration/install-on-linux/),
enable authentication (`security.authorization: enabled` in `/etc/mongod.conf`),
then create a user for Logsman:

```bash
mongosh admin --eval 'db.createUser({user:"admin", pwd:passwordPrompt(), roles:["root"]})'   # once, before enabling auth
sudo systemctl restart mongod
mongosh -u admin -p --authenticationDatabase admin --eval '
  db.getSiblingDB("logsman").createUser({user:"logsman", pwd:passwordPrompt(),
                                         roles:[{role:"readWrite", db:"logsman"}]})'
```

### 2. User, code and dependencies

```bash
sudo apt install -y python3 python3-venv git
sudo useradd --system --home-dir /opt/logsman --shell /usr/sbin/nologin logsman
sudo git clone https://github.com/TechWithDunamix/logsman.git /opt/logsman
sudo chown -R logsman:logsman /opt/logsman
cd /opt/logsman
sudo -u logsman python3 -m venv .venv
sudo -u logsman .venv/bin/pip install --upgrade pip
sudo -u logsman .venv/bin/pip install -r requirements.txt
```

### 3. Settings

```bash
sudo mkdir -p /etc/logsman
sudo cp deploy/logsman.env.example /etc/logsman/logsman.env
sudo chown root:logsman /etc/logsman/logsman.env && sudo chmod 0640 /etc/logsman/logsman.env
sudoedit /etc/logsman/logsman.env
```

Set `LOGSMAN_DEBUG=false`, the MongoDB URL with the `logsman` user's password
(URL-encode special characters), the BEACN URL and the consumer key. Keep
comments on their own lines: systemd does not strip trailing `# comments`.

### 4. Service

```bash
sudo cp deploy/systemd/logsman.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now logsman
curl --fail http://127.0.0.1:8010/api/health
journalctl -u logsman -f
```

The unit runs one uvicorn process on `127.0.0.1:8010` with systemd hardening.
Run **one** consumer: every extra worker or replica opens another subscription
and repeats each write (harmless, writes are idempotent, but wasted). To scale
reads, run additional instances with `LOGSMAN_BEACN_ENABLED=false`.

### 5. Reverse proxy with authentication

Because the API itself is unauthenticated, expose it only through a proxy that
authenticates, for example nginx with basic auth:

```nginx
server {
    listen 443 ssl http2;
    server_name logs.example.com;
    auth_basic "Logsman";
    auth_basic_user_file /etc/nginx/logsman.htpasswd;   # htpasswd -c … <user>
    location / {
        proxy_pass http://127.0.0.1:8010;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### 6. Upgrades and backups

```bash
cd /opt/logsman && sudo -u logsman git pull
sudo -u logsman .venv/bin/pip install -r requirements.txt
sudo systemctl restart logsman

mongodump --uri 'mongodb://logsman:…@127.0.0.1:27017/logsman?authSource=logsman' --out backup-$(date +%F)
```

MongoDB grows without bound; add a TTL index or an archival job if you need
retention. See [docs/OPERATIONS.md](docs/OPERATIONS.md).

---

## Run with Docker Compose

```bash
cp .env.example .env
# Set LOGSMAN_BEACN_API_KEY to a real BEACN consumer key and review LOGSMAN_BEACN_URL.
docker compose up --build -d
curl --fail http://127.0.0.1:8010/api/health
```

Compose runs MongoDB 8 (persisted in the `logsman-mongo` volume, no host port)
and Logsman on port 8010. The example settings reach BEACN on the Docker host
at `http://host.docker.internal:8000`. On Linux, a BEACN bound only to host
loopback may not be reachable from the bridge network; use a reachable,
access-controlled address or a shared Docker network. The bundled MongoDB has no
authentication, so keep this setup on a trusted network.

---

## Delivery limits

MongoDB writes are insert-once upserts keyed by the payload `id`, so a
redelivered event never replaces the first stored copy. The consumer reconnects
after stream errors, but its checkpoint lives only in memory and BEACN's SSE
route does not replay from `Last-Event-ID`. Events published while Logsman is
disconnected can therefore be missing: this is not a lossless or exactly-once
pipeline. See [recovery](docs/OPERATIONS.md).

---

## Develop and test

```bash
pip install -r requirements-dev.txt
pytest          # in-memory store and fake listeners; no MongoDB or BEACN needed
ruff check .
```

The tests cover payload validation, the read/filter endpoints, health and the
consumer's persistence. Smoke-test a real deployment's networking and
credentials separately.
