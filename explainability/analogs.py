from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

HISTORICAL_EVENTS = [
    {"name": "2008 Financial Crisis", "type": "economic", "impact": "severe", "sentiment": -0.9},
    {"name": "COVID-19 Pandemic", "type": "health", "impact": "severe", "sentiment": -0.8},
    {"name": "Russia-Ukraine War 2022", "type": "conflict", "impact": "high", "sentiment": -0.7},
    {"name": "Brexit Vote 2016", "type": "political", "impact": "high", "sentiment": -0.5},
    {"name": "US-China Trade War", "type": "trade", "impact": "moderate", "sentiment": -0.4},
    {"name": "Arab Spring 2011", "type": "political", "impact": "moderate", "sentiment": -0.3},
    {"name": "Tech Bubble 2000", "type": "market", "impact": "severe", "sentiment": -0.6},
    {"name": "Oil Price Crash 2014", "type": "energy", "impact": "moderate", "sentiment": -0.5},
    {"name": "European Debt Crisis 2011", "type": "economic", "impact": "high", "sentiment": -0.6},
    {"name": "Gulf War 1991", "type": "conflict", "impact": "moderate", "sentiment": -0.4},
]


class HistoricalAnalogsStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("historical_analogs")

    async def run(self, event: Event, context: Context) -> Event:
        current_sentiment = event.data.get("feature_aggregates", {}).get("avg_sentiment", 0)
        current_type = context.params.get("event_type", "economic")

        analogs = []
        for he in HISTORICAL_EVENTS:
            sentiment_sim = 1 - abs(he["sentiment"] - current_sentiment)
            type_match = 1.0 if he["type"] == current_type else 0.3
            score = sentiment_sim * 0.6 + type_match * 0.4
            analogs.append({
                "name": he["name"],
                "type": he["type"],
                "impact": he["impact"],
                "similarity_score": round(score, 4),
                "historical_sentiment": he["sentiment"],
            })

        analogs.sort(key=lambda x: x["similarity_score"], reverse=True)
        top_analogs = analogs[:5]

        event.data["historical_analogs"] = top_analogs
        event.data["analog_count"] = len(top_analogs)
        logger.info("Found %d historical analogs", len(top_analogs))
        return event


class HistoricalAnalogsPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="explainability_analogs",
            stages=[HistoricalAnalogsStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"analogs": result.data.get("analog_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
