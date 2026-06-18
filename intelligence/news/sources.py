from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

SOURCE_WEIGHTS = {
    "reuters": 0.95,
    "bloomberg": 0.95,
    "associated_press": 0.90,
    "bbc": 0.85,
    "cnn": 0.75,
    "al_jazeera": 0.70,
    "xinhua": 0.50,
    "rt": 0.30,
    "unknown": 0.50,
}


class NewsSourceStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("news_source")

    async def run(self, event: Event, context: Context) -> Event:
        articles = event.data.get("classified_articles") or event.data.get("articles", [])
        sourced = []

        for article in articles:
            source_name = (
                article.get("source_name")
                or article.get("source", "")
                or article.get("data", {}).get("source", "")
            )
            source_key = source_name.lower().replace(" ", "_") if source_name else "unknown"
            credibility = SOURCE_WEIGHTS.get(source_key, SOURCE_WEIGHTS.get("unknown", 0.5))
            article["source_credibility"] = credibility
            article["source_weight"] = credibility
            sourced.append(article)

        event.data["sourced_articles"] = sourced
        return event


class NewsSourcePipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="news_sources",
            stages=[NewsSourceStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(context=context, status=PipelineStatus.SUCCESS, events=[result])
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
