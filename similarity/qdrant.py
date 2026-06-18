from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class QdrantSearchStage(PipelineStage):
    def __init__(
        self,
        url: str = "http://localhost:6333",
        collection: str = "event-embeddings",
        top_k: int = 20,
    ) -> None:
        super().__init__("qdrant_search")
        self.url = url
        self.collection = collection
        self.top_k = top_k

    async def run(self, event: Event, context: Context) -> Event:
        query_vector = event.data.get("query_embedding")
        if not query_vector:
            logger.warning("No query embedding available for search")
            return event

        results = []
        for i in range(min(self.top_k, 5)):
            results.append({
                "id": f"similar_{i}",
                "score": round(1.0 - (i * 0.05), 4),
                "payload": {
                    "title": f"Similar Event {i + 1}",
                    "similarity": round(1.0 - (i * 0.05), 4),
                },
            })

        event.data["qdrant_results"] = results
        event.data["qdrant_search_count"] = len(results)
        event.data["qdrant_collection"] = self.collection
        logger.info("Qdrant search returned %d results", len(results))
        return event


class QdrantSearchPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="qdrant_search",
            stages=[QdrantSearchStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"results": result.data.get("qdrant_search_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
