"""The payload contract accepted from the BEACN ``logs`` topic."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EVENT_TYPES = ("error", "info", "debug", "actions")


class EventModel(BaseModel):
    """A log event carried in the ``data`` field of a BEACN event."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["error", "info", "debug", "actions"]
    source: str = Field(min_length=1)
    destination: str = Field(min_length=1)
    id: str = Field(min_length=1)
    data: Any = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
