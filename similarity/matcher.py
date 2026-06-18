from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class EventMatcherStage(PipelineStage):
    def __init__(self, top_k: int = 20) -> None:
        super().__init__("event_matcher")
        self.top_k = top_k

    async def run(self, event: Event, context: Context) -> Event:
        qdrant_results = event.data.get("qdrant_results", [])
        threshold = context.params.get("similarity_threshold", 0.7)

        matches = []
        for r in qdrant_results:
            if r.get("score", 0) >= threshold:
                matches.append({
                    "matched_event_id": r["id"],
                    "similarity_score": r["score"],
                    "payload": r.get("payload", {}),
                })

        matches.sort(key=lambda x: x["similarity_score"], reverse=True)
        top_matches = matches[:self.top_k]

        event.data["matched_events"] = top_matches
        event.data["match_count"] = len(top_matches)
        event.data["match_threshold"] = threshold

        logger.info("Matched %d events (threshold=%.2f)", len(top_matches), threshold)
        return event


class EventMatcherPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="event_matcher",
            stages=[EventMatcherStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"matches": result.data.get("match_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
