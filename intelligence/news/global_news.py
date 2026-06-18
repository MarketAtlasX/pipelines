from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.collector import GDELTCollector, RSSCollector
from pipelines.data_factory.cleaner import PipelineCleaner, Deduplicator, Normalizer, Validator
from pipelines.data_factory.transformer import EventBuilder, PipelineTransformer
from pipelines.data_factory.enricher import GeoEnricher, EntityLinker, TemporalEnricher, PipelineEnricher

logger = logging.getLogger(__name__)

NEWS_CATEGORIES = [
    "geopolitics", "economy", "markets", "technology", "energy",
    "defense", "trade", "health", "environment", "conflict",
]


class GlobalNewsCollectStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("global_news_collect")
        self.gdelt = GDELTCollector()
        self.rss = RSSCollector([])

    async def run(self, event: Event, context: Context) -> Event:
        gdelt_events = await self.gdelt.collect_batch(100)
        articles = [e.model_dump() for e in gdelt_events]
        event.data["articles"] = articles
        event.data["article_count"] = len(articles)
        event.data["source"] = "global_news_pipeline"
        logger.info("Global news collected %d articles", len(articles))
        return event


class GlobalNewsClassifyStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("global_news_classify")

    async def run(self, event: Event, context: Context) -> Event:
        articles = event.data.get("articles", [])
        for article in articles:
            text = f"{article.get('title', '')} {article.get('content', '')}".lower()
            categories = []
            for cat in NEWS_CATEGORIES:
                kw_map = {
                    "geopolitics": ["diplomat", "treaty", "alliance", "ambassador", "sovereign"],
                    "economy": ["economy", "gdp", "inflation", "central bank", "fiscal"],
                    "markets": ["stock", "bond", "market", "index", "volatility"],
                    "technology": ["tech", "ai", "cyber", "semiconductor", "software"],
                    "energy": ["oil", "gas", "energy", "renewable", "nuclear"],
                    "defense": ["military", "defense", "weapon", "army", "navy"],
                    "trade": ["trade", "tariff", "export", "import", "supply chain"],
                    "health": ["health", "pandemic", "vaccine", "disease", "hospital"],
                    "environment": ["climate", "environment", "emission", "flood", "drought"],
                    "conflict": ["conflict", "war", "violence", "protest", "sanctions"],
                }
                if any(kw in text for kw in kw_map.get(cat, [])):
                    categories.append(cat)
            article["categories"] = categories or ["general"]
            article["category_count"] = len(article["categories"])

        event.data["classified_articles"] = articles
        return event


class GlobalNewsPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="global_news",
            stages=[
                GlobalNewsCollectStage(),
                GlobalNewsClassifyStage(),
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
                metrics={"articles": result.data.get("article_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
