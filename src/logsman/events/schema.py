"""The payload contract accepted from the BEACN ``logs`` topic.

A publisher sends a normal BEACN event whose ``data`` field is a log object::

    {
      "event": "log.created",
      "topic": "logs",
      "data": {
        "id": "log_123",              # archive key, unique per log
        "type": "error",              # error | info | debug | actions
        "source": "checkout",         # who emitted the log
        "destination": "payments",    # what it concerns / where it was headed
        "data": {"message": "card declined"},   # any JSON value
        "tags": ["payments", "urgent"]
      }
    }

Only the inner object is validated here; the BEACN envelope is validated by
BEACN itself and stored alongside as metadata.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

#: Every value ``EventModel.type`` accepts, in the order the API documents them.
EVENT_TYPES = ("error", "info", "debug", "actions")


class EventModel(BaseModel):
    """A log event carried in the ``data`` field of a BEACN event.

    Validation is strict: unknown fields are rejected (``extra="forbid"``) so a
    typo such as ``"tag"`` instead of ``"tags"`` fails loudly instead of being
    silently dropped. Invalid payloads are discarded by the consumer and
    reported in its ``last_error``.

    Attributes:
        type: Log category: ``error``, ``info``, ``debug`` or ``actions``.
        source: Non-empty name of the emitting service or component.
        destination: Non-empty name of the target service, resource or audience.
        id: Non-empty, caller-chosen unique id. It is the archive's primary key:
            a second event with the same id is ignored, which makes publisher
            retries safe. It is unrelated to BEACN's own ``evt_…`` envelope id.
        data: Free-form JSON payload (object, list, string, number…). Defaults
            to an empty object.
        tags: Free-form labels used for filtering. Defaults to an empty list.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["error", "info", "debug", "actions"]
    source: str = Field(min_length=1)
    destination: str = Field(min_length=1)
    id: str = Field(min_length=1)
    data: Any = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
