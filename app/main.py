"""ASGI entrypoint for ``uvicorn app.main:app``."""

from app.bootstrap import create_app
from app.config import config

app = create_app()


if __name__ == "__main__":
    app.run(host=config.host, port=config.port, reload=config.debug)
