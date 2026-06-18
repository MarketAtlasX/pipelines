from __future__ import annotations

import logging
from typing import Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.collector import NewsAPICollector
from pipelines.data_factory.cleaner import PipelineCleaner

logger = logging.getLogger(__name__)


class NewsAPIFetchStage(PipelineStage):
    def __init__(self, api_key: str) -> None:
        super().__init__("newsapi_fetch")
        self.collector = NewsAPICollector(api_key)

    async def run(self, event: Event, context: Context) -> Event:
        events = await self.collector.collect_batch(max_events=100)
        event.data["raw_events"] = [e.model_dump() for e in events]
        event.data["raw_count"] = len(events)
        logger.info("NewsAPI fetch: %d articles", len(events))
        return event


class NewsAPICleanStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("newsapi_clean")
        self.cleaner = PipelineCleaner()

    async def run(self, event: Event, context: Context) -> Event:
        raw = event.data.get("raw_events", [])
        cleaned = []
        for raw_dict in raw:
            raw_event = Event(**raw_dict)
            result = await self.cleaner.clean(raw_event)
            if result:
                cleaned.append(result.model_dump())
        event.data["cleaned_events"] = cleaned
        event.data["cleaned_count"] = len(cleaned)
        return event


class NewsAPIPipeline(Pipeline):
    def __init__(self, api_key: str) -> None:
        super().__init__(
            name="newsapi_ingestion",
            stages=[
                NewsAPIFetchStage(api_key),
                NewsAPICleanStage(),
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
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
