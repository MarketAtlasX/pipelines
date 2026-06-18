from __future__ import annotations

import hashlib
import logging
from typing import Set

from pipelines.core.base import PipelineStage
from pipelines.core.types import Context, Event

logger = logging.getLogger(__name__)


class DedupStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("daily_dedup")
        self._seen: Set[str] = set()

    async def run(self, event: Event, context: Context) -> Event:
        events = event.data.get("articles") or event.data.get("raw_events") or [event.model_dump()]
        deduped = []
        skipped = 0

        for e in events:
            if not isinstance(e, dict):
                continue
            key = hashlib.md5(
                f"{e.get('title', '')}{e.get('url', '')}".encode()
            ).hexdigest()
            if key not in self._seen:
                self._seen.add(key)
                e["dedup_key"] = key
                deduped.append(e)
            else:
                skipped += 1

        event.data["deduped_events"] = deduped
        event.data["dedup_count"] = len(deduped)
        event.data["dedup_skipped"] = skipped
        logger.info("Dedup: %d kept, %d skipped", len(deduped), skipped)
        return event
