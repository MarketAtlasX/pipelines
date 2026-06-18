from __future__ import annotations

import abc
import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from pipelines.core.types import Event

logger = logging.getLogger(__name__)


class Cleaner(abc.ABC):
    @abc.abstractmethod
    async def clean(self, event: Event) -> Optional[Event]: ...


class Deduplicator(Cleaner):
    def __init__(self) -> None:
        self._seen: Set[str] = set()

    async def clean(self, event: Event) -> Optional[Event]:
        key = hashlib.md5(
            str(event.data.get("title", "")).encode() + str(event.data.get("url", "")).encode()
        ).hexdigest()
        if key in self._seen:
            logger.debug("Dedup: skipped duplicate event %s", event.id)
            return None
        self._seen.add(key)
        event.metadata["dedup_key"] = key
        return event


class Normalizer(Cleaner):
    async def clean(self, event: Event) -> Optional[Event]:
        text = event.data.get("content") or event.data.get("description") or ""
        cleaned = re.sub(r"\s+", " ", text).strip()
        title = (event.data.get("title") or "").strip()
        event.data["title"] = title
        event.data["content"] = cleaned
        event.data["text_length"] = len(cleaned)
        return event


class Validator(Cleaner):
    MIN_LENGTH = 20

    async def clean(self, event: Event) -> Optional[Event]:
        content = event.data.get("content") or event.data.get("description") or ""
        title = event.data.get("title") or ""
        if not title and not content:
            logger.debug("Validation: skipped empty event %s", event.id)
            return None
        if len(content) < self.MIN_LENGTH and len(title) < 3:
            logger.debug("Validation: skipped short event %s", event.id)
            return None
        return event


class PipelineCleaner(Cleaner):
    def __init__(self, cleaners: Optional[List[Cleaner]] = None) -> None:
        self.cleaners = cleaners or [Deduplicator(), Normalizer(), Validator()]

    def add(self, cleaner: Cleaner) -> PipelineCleaner:
        self.cleaners.append(cleaner)
        return self

    async def clean(self, event: Event) -> Optional[Event]:
        current: Optional[Event] = event
        for cleaner in self.cleaners:
            if current is None:
                return None
            current = await cleaner.clean(current)
        return current
