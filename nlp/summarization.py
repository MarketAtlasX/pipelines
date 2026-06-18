from __future__ import annotations

import logging
from typing import Any, Dict, List

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class SummarizationStage(PipelineStage):
    def __init__(self, max_summary_sentences: int = 3) -> None:
        super().__init__("summarization")
        self.max_sentences = max_summary_sentences

    async def run(self, event: Event, context: Context) -> Event:
        events = event.data.get("sentiment_events") or event.data.get("cleaned_events") or [event.model_dump()]
        for e in events:
            if not isinstance(e, dict):
                continue
            content = e.get("content") or e.get("description") or ""
            title = e.get("title", "")
            e["summary"] = self._extractive_summary(f"{title}. {content}")
        event.data["summarized_events"] = events
        return event

    def _extractive_summary(self, text: str) -> str:
        import re
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        if len(sentences) <= self.max_sentences:
            return text
        scored = []
        for s in sentences:
            score = len(s.split())
            if any(kw in s.lower() for kw in ["market", "economy", "conflict", "trade", "risk", "growth", "crisis"]):
                score += 10
            scored.append((score, s))
        scored.sort(reverse=True, key=lambda x: x[0])
        top = [s for _, s in scored[:self.max_sentences]]
        return " ".join(top)


class SummarizationPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="nlp_summarization",
            stages=[SummarizationStage()],
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
