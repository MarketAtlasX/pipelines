from __future__ import annotations

import abc
import json
import logging
from typing import Any, Dict, List, Optional

from pipelines.core.types import Event

logger = logging.getLogger(__name__)


class Store(abc.ABC):
    @abc.abstractmethod
    async def write(self, event: Event) -> bool: ...

    @abc.abstractmethod
    async def write_batch(self, events: List[Event]) -> int: ...


class PostgresStore(Store):
    def __init__(self, connection_url: str) -> None:
        self.connection_url = connection_url

    async def write(self, event: Event) -> bool:
        logger.debug("PostgresStore: writing event %s", event.id)
        return True

    async def write_batch(self, events: List[Event]) -> int:
        logger.info("PostgresStore: batch writing %d events", len(events))
        return len(events)


class RedisStore(Store):
    def __init__(self, redis_url: str) -> None:
        self.redis_url = redis_url

    async def write(self, event: Event) -> bool:
        logger.debug("RedisStore: caching event %s", event.id)
        return True

    async def write_batch(self, events: List[Event]) -> int:
        return len(events)


class S3Store(Store):
    def __init__(self, bucket: str, prefix: str = "events/") -> None:
        self.bucket = bucket
        self.prefix = prefix

    async def write(self, event: Event) -> bool:
        logger.debug("S3Store: archiving event %s to %s%s", event.id, self.prefix, event.id)
        return True

    async def write_batch(self, events: List[Event]) -> int:
        logger.info("S3Store: archiving %d events", len(events))
        return len(events)


class QdrantStore(Store):
    def __init__(self, url: str, collection: str = "event-embeddings") -> None:
        self.url = url
        self.collection = collection

    async def write(self, event: Event) -> bool:
        logger.debug("QdrantStore: upserting event %s", event.id)
        return True

    async def write_batch(self, events: List[Event]) -> int:
        logger.info("QdrantStore: upserting %d vectors", len(events))
        return len(events)
