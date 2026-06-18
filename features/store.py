from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.store import RedisStore

logger = logging.getLogger(__name__)


class FeatureStoreStage(PipelineStage):
    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        super().__init__("feature_store")
        self.store = RedisStore(redis_url)

    async def run(self, event: Event, context: Context) -> Event:
        features = event.data.get("features", [])
        signals = event.data.get("signals", [])
        aggregates = event.data.get("feature_aggregates", {})

        payload = {
            "features": features,
            "signals": signals,
            "aggregates": aggregates,
            "timestamp": event.timestamp.isoformat(),
            "event_id": event.id,
        }

        await self.store.write(event)
        event.data["stored_features"] = True
        logger.info("Stored %d features and %d signals", len(features), len(signals))
        return event


class FeatureStorePipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="feature_store",
            stages=[FeatureStoreStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
