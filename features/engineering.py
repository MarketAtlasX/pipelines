from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import numpy as np

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class FeatureExtractionStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("feature_extraction")

    async def run(self, event: Event, context: Context) -> Event:
        events = event.data.get("graph_analysis") or event.data.get("sentiment_events") or [event.model_dump()]
        features = []

        for e in events if isinstance(events, list) else [events]:
            if not isinstance(e, dict):
                continue
            f = {
                "sentiment_score": e.get("sentiment", {}).get("score", 0) if isinstance(e.get("sentiment"), dict) else 0,
                "sentiment_label": e.get("sentiment", {}).get("label", "neutral") if isinstance(e.get("sentiment"), dict) else "neutral",
                "entity_count": len(e.get("entities", {}).get("countries", [])) + len(e.get("entities", {}).get("organizations", [])) if isinstance(e.get("entities"), dict) else 0,
                "text_length": len(e.get("content", "") or ""),
                "has_geo": 1 if e.get("locations") else 0,
                "hour_of_day": self._get_hour(e.get("published", "")),
                "day_of_week": self._get_dow(e.get("published", "")),
            }
            if "embedding" in e:
                f["has_embedding"] = 1
                if isinstance(e.get("embedding"), list) and len(e["embedding"]) > 0:
                    f["embedding_mean"] = float(np.mean(e["embedding"]))
                    f["embedding_std"] = float(np.std(e["embedding"]))
            features.append(f)

        event.data["features"] = features
        event.data["feature_count"] = len(features)
        logger.info("Extracted %d feature vectors", len(features))
        return event

    def _get_hour(self, published: str) -> int:
        try:
            dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            return dt.hour
        except (ValueError, AttributeError):
            return 0

    def _get_dow(self, published: str) -> int:
        try:
            dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            return dt.weekday()
        except (ValueError, AttributeError):
            return 0


class FeatureAggregationStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("feature_aggregation")

    async def run(self, event: Event, context: Context) -> Event:
        features = event.data.get("features", [])
        if not features:
            return event

        aggregated = {
            "total_events": len(features),
            "avg_sentiment": float(np.mean([f["sentiment_score"] for f in features])),
            "positive_ratio": sum(1 for f in features if f["sentiment_label"] == "positive") / max(len(features), 1),
            "negative_ratio": sum(1 for f in features if f["sentiment_label"] == "negative") / max(len(features), 1),
            "avg_entity_count": float(np.mean([f["entity_count"] for f in features])),
            "geo_ratio": sum(f["has_geo"] for f in features) / max(len(features), 1),
        }

        event.data["feature_aggregates"] = aggregated
        logger.info("Feature aggregation: %s", aggregated)
        return event


class FeatureEngineeringPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="feature_engineering",
            stages=[
                FeatureExtractionStage(),
                FeatureAggregationStage(),
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
                metrics={
                    "features": result.data.get("feature_count", 0),
                    "avg_sentiment": result.data.get("feature_aggregates", {}).get("avg_sentiment", 0),
                },
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
