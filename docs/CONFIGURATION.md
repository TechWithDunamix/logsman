# Configuration

Settings are defined in `app/config.py`. All use the `LOGSMAN_` prefix and are
loaded from the process environment and the `.env` file in the working directory.
The application explicitly constructs `AppConfig(_env_file=".env")`.
Never commit `.env` or API keys.

| Variable | Built-in default | Meaning |
| --- | --- | --- |
| `LOGSMAN_APP_NAME` | `Logsman` | Application/OpenAPI title |
| `LOGSMAN_APP_ENV` | `local` | `local`, `testing`, `staging` or `production` label |
| `LOGSMAN_DEBUG` | `true` | Sillo debug mode; turn off for production |
| `LOGSMAN_HOST` | `127.0.0.1` | Bind address when running `python -m app.main` |
| `LOGSMAN_PORT` | `8010` | Port when running `python -m app.main` |
| `LOGSMAN_MONGODB_URL` | `mongodb://localhost:27017` | MongoDB connection URI |
| `LOGSMAN_MONGODB_DATABASE` | `logsman` | Database name |
| `LOGSMAN_MONGODB_COLLECTION` | `events` | Collection name |
| `LOGSMAN_BEACN_URL` | `http://localhost:8000` | BEACN base HTTP URL |
| `LOGSMAN_BEACN_API_KEY` | empty | Consumer key; nonempty required when enabled |
| `LOGSMAN_BEACN_TOPIC` | `logs` | Single topic consumed by this process |
| `LOGSMAN_BEACN_ENABLED` | `true` | Start the background listener |

The `.env.example` is container-oriented: MongoDB uses `mongo` and BEACN uses
`host.docker.internal`. These differ from the built-in defaults. For local
Python development, set the two URLs to host-reachable services explicitly.
Inside a container, `localhost` points at that container, not your host.

When starting with Uvicorn, its command-line `--host` and `--port` flags control
the socket; `LOGSMAN_HOST`/`LOGSMAN_PORT` do not override them. The Docker image
runs Uvicorn on `0.0.0.0:8010` regardless of those two settings.

Setting `LOGSMAN_APP_ENV=production` alone does not enforce production safety
checks or change debug mode. Set `LOGSMAN_DEBUG=false` explicitly, protect the
read API, and use an authenticated MongoDB URI as appropriate for your hosting.

The consumer key needs `events:read` to mint its realtime token. Publishers use
`events:publish`; use separate keys. For recovery tooling, grant `events:replay`
to the tool doing the backfill. All credentials and producers must share the
intended BEACN environment. Topic authorization is enforced by BEACN.
