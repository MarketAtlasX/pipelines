"""EventSimilarityPipeline — the specific one the user asked for.

New Event → Embedding Model → Qdrant Search → Top 20 Similar Events
→ Market Outcome Retrieval → Store Similarity Links
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.similarity.embedding import SimilarityEmbeddingStage
from pipelines.similarity.qdrant import QdrantSearchStage
from pipelines.similarity.matcher import EventMatcherStage
from pipelines.similarity.links import MarketOutcomeRetrievalStage, StoreSimilarityLinksStage

logger = logging.getLogger(__name__)


class EventSimilarityPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="event_similarity",
            stages=[
                SimilarityEmbeddingStage(),
                QdrantSearchStage(top_k=20),
                EventMatcherStage(top_k=20),
                MarketOutcomeRetrievalStage(),
                StoreSimilarityLinksStage(),
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
                    "matches": result.data.get("match_count", 0),
                    "links_stored": result.data.get("links_stored", 0),
                },
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            logger.exception("EventSimilarityPipeline failed")
            outcome = Outcome(
                context=context,
                status=PipelineStatus.FAILED,
                error=str(e),
            )
            self.state.fail(outcome)
            return outcome
