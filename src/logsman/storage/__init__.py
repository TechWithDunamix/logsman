"""Event storage.

* :class:`~logsman.storage.base.EventStore` — the protocol every store implements.
* :class:`~logsman.storage.mongo.MongoEventStore` — the production MongoDB store.
* :class:`~logsman.storage.memory.MemoryEventStore` — a process-local store for
  tests and local experiments.
"""

from logsman.storage.base import EventStore
from logsman.storage.memory import MemoryEventStore
from logsman.storage.mongo import MongoEventStore

__all__ = ["EventStore", "MemoryEventStore", "MongoEventStore"]
