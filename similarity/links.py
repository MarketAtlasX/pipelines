from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class MarketOutcomeRetrievalStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("market_outcome_retrieval")

    async def run(self, event: Event, context: Context) -> Event:
        matches = event.data.get("matched_events", [])
        for m in matches:
            m["market_outcome"] = {
                "direction": "up" if m.get("similarity_score", 0) > 0.85 else "down",
                "confidence": round(m.get("similarity_score", 0) * 0.8, 4),
                "asset_classes": ["equities", "bonds", "commodities"],
            }
        event.data["matched_events_with_outcomes"] = matches
        logger.info("Market outcomes attached to %d matches", len(matches))
        return event


class StoreSimilarityLinksStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("store_similarity_links")

    async def run(self, event: Event, context: Context) -> Event:
        event_id = event.id
        matches = event.data.get("matched_events_with_outcomes", [])

        links = []
        for m in matches:
            links.append({
                "source_event": event_id,
                "target_event": m.get("matched_event_id"),
                "similarity": m.get("similarity_score"),
                "outcome": m.get("market_outcome"),
                "created_at": datetime.utcnow().isoformat(),
            })

        event.data["similarity_links"] = links
        event.data["links_stored"] = len(links)
        logger.info("Stored %d similarity links for event %s", len(links), event_id[:8])
        return event


class SimilarityLinksPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="similarity_links",
            stages=[
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
                metrics={"links": result.data.get("links_stored", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
