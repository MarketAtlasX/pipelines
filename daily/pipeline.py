"""DailyPipeline — GDELT → Deduplication → Enrichment → Graph Update → Feature Generation → Signal Generation → Store Results

The user's specified Daily Pipeline flow.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from pipelines.core.base import Pipeline
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class DailyPipeline(Pipeline):
    def __init__(self) -> None:
        from pipelines.daily.dedup import DedupStage
        from pipelines.daily.enrichment import DailyEnrichmentStage
        from pipelines.daily.graph_update import DailyGraphUpdateStage
        from pipelines.daily.features import DailyFeatureStage
        from pipelines.daily.signals import DailySignalStage
        from pipelines.daily.store import DailyStoreStage

        super().__init__(
            name="daily_pipeline",
            stages=[
                DedupStage(),
                DailyEnrichmentStage(),
                DailyGraphUpdateStage(),
                DailyFeatureStage(),
                DailySignalStage(),
                DailyStoreStage(),
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
                    "events_processed": 1,
                    "signals": len(result.data.get("signals", [])),
                    "features": len(result.data.get("features", [])),
                },
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            logger.exception("DailyPipeline failed")
            outcome = Outcome(
                context=context,
                status=PipelineStatus.FAILED,
                error=str(e),
            )
            self.state.fail(outcome)
            return outcome
