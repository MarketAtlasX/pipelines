from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class SentimentStage(PipelineStage):
    POSITIVE_WORDS = {
        "growth", "profit", "surplus", "gain", "rally", "boom", "recovery",
        "breakthrough", "innovation", "expansion", "prosperity", "opportunity",
        "stable", "strong", "positive", "optimistic", "upgrade", "bullish",
        "victory", "success", "achievement", "progress", "improvement",
    }
    NEGATIVE_WORDS = {
        "crisis", "crash", "loss", "deficit", "decline", "recession", "inflation",
        "default", "bankruptcy", "sanctions", "conflict", "war", "violence",
        "casualty", "death", "destruction", "threat", "risk", "downgrade",
        "bearish", "collapse", "instability", "turmoil", "downturn", "slump",
        "emergency", "disaster", "attack", "hostility", "tension",
    }

    def __init__(self) -> None:
        super().__init__("sentiment_analysis")

    async def run(self, event: Event, context: Context) -> Event:
        events = (
            event.data.get("entity_extracted_events")
            or event.data.get("cleaned_events")
            or [event.model_dump()]
        )
        for e in events:
            if not isinstance(e, dict):
                continue
            text = f"{e.get('title', '')} {e.get('content', '')}".lower()
            words = set(text.split())
            pos_count = len(words & self.POSITIVE_WORDS)
            neg_count = len(words & self.NEGATIVE_WORDS)
            total = pos_count + neg_count
            score = (pos_count - neg_count) / max(total, 1)
            e["sentiment"] = {
                "score": round(score, 4),
                "label": "positive" if score > 0.1 else ("negative" if score < -0.1 else "neutral"),
                "positive_words": pos_count,
                "negative_words": neg_count,
            }
        event.data["sentiment_events"] = events
        return event


class SentimentPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="nlp_sentiment",
            stages=[SentimentStage()],
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
