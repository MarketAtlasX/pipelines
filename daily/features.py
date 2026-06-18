from __future__ import annotations

import logging

import numpy as np

from pipelines.core.base import PipelineStage
from pipelines.core.types import Context, Event

logger = logging.getLogger(__name__)


class DailyFeatureStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("daily_features")

    async def run(self, event: Event, context: Context) -> Event:
        events_data = event.data.get("enriched_events", [])
        features = []

        for e in events_data:
            if not isinstance(e, dict):
                continue
            entities = e.get("entities", {}) if isinstance(e.get("entities"), dict) else {}
            sentiment = e.get("sentiment", {}) if isinstance(e.get("sentiment"), dict) else {}
            f = {
                "sentiment_score": sentiment.get("score", 0),
                "entity_count": len(entities.get("countries", [])) + len(entities.get("organizations", [])),
                "text_length": len(e.get("content", "") or ""),
                "has_geo": 1 if e.get("locations") else 0,
            }
            features.append(f)

        if features:
            event.data["features"] = features
            event.data["daily_feature_aggregates"] = {
                "avg_sentiment": float(np.mean([f["sentiment_score"] for f in features])),
                "avg_entities": float(np.mean([f["entity_count"] for f in features])),
                "total_events": len(features),
            }

        logger.info("Generated %d feature vectors", len(features))
        return event
