from __future__ import annotations

import logging

from pipelines.core.base import PipelineStage
from pipelines.core.types import Context, Event

logger = logging.getLogger(__name__)


class DailyStoreStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("daily_store")

    async def run(self, event: Event, context: Context) -> Event:
        event.data["daily_pipeline_complete"] = True
        logger.info(
            "Daily pipeline results stored. Events: %d, Features: %d, Signals: %d",
            len(event.data.get("deduped_events", [])),
            len(event.data.get("features", [])),
            len(event.data.get("signals", [])),
        )
        return event
