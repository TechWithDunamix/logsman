"""Typed configuration for Logsman."""

from __future__ import annotations

from typing import Literal

from sillo.config import Config


class AppConfig(Config):
    class Env:
        env_prefix = "LOGSMAN_"

    app_name: str = "Logsman"
    app_env: Literal["local", "testing", "staging", "production"] = "local"
    debug: bool = True
    host: str = "127.0.0.1"
    port: int = 8010

    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "logsman"
    mongodb_collection: str = "events"

    beacn_url: str = "http://localhost:8000"
    beacn_api_key: str = ""
    beacn_topic: str = "logs"
    beacn_enabled: bool = True


config = AppConfig(_env_file=".env")
