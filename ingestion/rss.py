from __future__ import annotations

import logging
from typing import List

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.collector import RSSCollector
from pipelines.data_factory.cleaner import PipelineCleaner
from pipelines.data_factory.transformer import EventBuilder, PipelineTransformer
from pipelines.data_factory.enricher import TemporalEnricher

logger = logging.getLogger(__name__)


class RSSFetchStage(PipelineStage):
    def __init__(self, feeds: List[str]) -> None:
        super().__init__("rss_fetch")
        self.collector = RSSCollector(feeds)

    async def run(self, event: Event, context: Context) -> Event:
        events = await self.collector.collect_batch(max_events=50)
        event.data["raw_events"] = [e.model_dump() for e in events]
        event.data["raw_count"] = len(events)
        logger.info("RSS fetch: %d items from %d feeds", len(events), len(self.collector.feeds))
        return event


class RSSTransformStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("rss_transform")
        self.transformer = PipelineTransformer([
            EventBuilder(),
            TemporalEnricher(),
        ])

    async def run(self, event: Event, context: Context) -> Event:
        raw = event.data.get("raw_events", [])
        transformed = []
        for raw_dict in raw:
            raw_event = Event(**raw_dict)
            result = await self.transformer.transform(raw_event)
            transformed.append(result.model_dump())
        event.data["transformed_events"] = transformed
        event.data["transformed_count"] = len(transformed)
        return event


class RSSPipeline(Pipeline):
    def __init__(self, feeds: List[str]) -> None:
        super().__init__(
            name="rss_ingestion",
            stages=[
                RSSFetchStage(feeds),
                RSSTransformStage(),
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
                metrics={"items_ingested": result.data.get("transformed_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
