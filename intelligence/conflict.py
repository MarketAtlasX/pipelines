from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.collector import GDELTCollector

logger = logging.getLogger(__name__)

CONFLICT_KEYWORDS = [
    "conflict", "war", "military", "violence", "protest", "sanctions",
    "ceasefire", "hostility", "insurgency", "rebellion", "invasion",
    "bombing", "strike", "offensive", "casualty", "refugee",
]

ESCALATION_TRIGGERS = {
    "high": ["invasion", "nuclear", "chemical weapon", "genocide", "massacre"],
    "medium": ["sanctions", "military exercise", "troop", "deployment", "ultimatum"],
    "low": ["diplomatic", "negotiation", "talk", "meeting", "summit"],
}


class ConflictDataSourceStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("conflict_data_source")

    async def run(self, event: Event, context: Context) -> Event:
        collector = GDELTCollector()
        raw = await collector.collect_conflict().__anext__()
        event.data["conflict_sources"] = ["gdelt", "rss", "newsapi"]
        return event


class ConflictAnalysisStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("conflict_analysis")

    async def run(self, event: Event, context: Context) -> Event:
        text = f"{event.data.get('title', '')} {event.data.get('content', '')}"
        escalation = "low"
        matched_triggers = []

        for level, triggers in ESCALATION_TRIGGERS.items():
            for t in triggers:
                if t.lower() in text.lower():
                    escalation = level
                    matched_triggers.append(t)

        zones = []
        for c in ["Ukraine", "Gaza", "Syria", "Yemen", "Sudan", "Myanmar", "Taiwan Strait", "Korean Peninsula"]:
            if c.lower() in text.lower():
                zones.append(c)

        event.data["conflict_intel"] = {
            "escalation_level": escalation,
            "matched_triggers": matched_triggers,
            "active_zones": zones,
            "keyword_hits": sum(1 for kw in CONFLICT_KEYWORDS if kw in text.lower()),
            "severity_score": sum(10 if escalation == "high" else 5 if escalation == "medium" else 1),
        }

        logger.info("Conflict analysis: escalation=%s, zones=%s", escalation, zones)
        return event


class ConflictIntelligencePipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="conflict_intelligence",
            stages=[
                ConflictDataSourceStage(),
                ConflictAnalysisStage(),
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
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
