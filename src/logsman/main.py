"""ASGI entry point.

Import target for ASGI servers::

    uvicorn logsman.main:app --host 0.0.0.0 --port 8010

The module builds exactly one application from the process environment (and
``.env`` in the working directory) at import time. Tests never import this
module; they call :func:`logsman.core.app.create_app` with explicit settings
and an in-memory store instead.
"""

from logsman.core.app import create_app

#: The application served by uvicorn. Built from :data:`logsman.core.config.config`.
app = create_app()
