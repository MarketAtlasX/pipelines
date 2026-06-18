from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.collector import GDELTCollector
from pipelines.data_factory.cleaner import PipelineCleaner
from pipelines.data_factory.store import PostgresStore, S3Store

logger = logging.getLogger(__name__)


class GDETFetchStage(PipelineStage):
    def __init__(self, api_url: str = "https://api.gdeltproject.org/api/v2/doc/doc", max_records: int = 250) -> None:
        super().__init__("gdelt_fetch")
        self.collector = GDELTCollector(api_url, max_records)

    async def run(self, event: Event, context: Context) -> Event:
        events = await self.collector.collect_batch(max_events=250)
        context.state["raw_articles"] = len(events)
        event.data["raw_events"] = [e.model_dump() for e in events]
        event.data["raw_count"] = len(events)
        logger.info("GDELT fetch: %d raw articles", len(events))
        return event


class GDELTCleanStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("gdelt_clean")
        self.cleaner = PipelineCleaner()

    async def run(self, event: Event, context: Context) -> Event:
        raw = event.data.get("raw_events", [])
        cleaned = []
        for raw_event_dict in raw:
            raw_event = Event(**raw_event_dict)
            result = await self.cleaner.clean(raw_event)
            if result:
                cleaned.append(result.model_dump())
        event.data["cleaned_events"] = cleaned
        event.data["cleaned_count"] = len(cleaned)
        logger.info("GDELT clean: %d -> %d", event.data["raw_count"], len(cleaned))
        return event


class GDELTPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="gdelt_ingestion",
            stages=[
                GDETFetchStage(),
                GDELTCleanStage(),
            ],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"articles_ingested": result.data.get("cleaned_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            logger.exception("GDELT pipeline failed")
            outcome = Outcome(
                context=context,
                status=PipelineStatus.FAILED,
                error=str(e),
            )
            self.state.fail(outcome)
            return outcome
