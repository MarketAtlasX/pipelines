from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

COUNTRY_KEYWORDS = [
    "USA", "United States", "China", "Russia", "Ukraine", "Iran", "Israel",
    "India", "Germany", "France", "UK", "Japan", "Saudi Arabia", "Turkey",
    "North Korea", "Syria", "Afghanistan", "Venezuela", "Brazil", "Pakistan",
    "Somalia", "Ethiopia", "Myanmar", "Sudan", "Yemen", "Iraq", "Afghanistan",
]

ORG_KEYWORDS = [
    r"\b(?:UN|NATO|EU|OPEC|IMF|World Bank|Fed|ECB|WHO|WTO|ICC|CIA|FBI|NASA)\b",
    r"\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)*(?:\s(?:Inc|Corp|LLC|Ltd|Group|Bank|Fund|Agency|Organization|Committee))\b",
]


class EntityExtractionStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("entity_extraction")
        self.org_patterns = [re.compile(p) for p in ORG_KEYWORDS]

    async def run(self, event: Event, context: Context) -> Event:
        events = event.data.get("cleaned_events") or event.data.get("embedded_events") or [event.model_dump()]
        for e in events:
            if not isinstance(e, dict):
                continue
            text = f"{e.get('title', '')} {e.get('content', '')}"
            countries = [c for c in COUNTRY_KEYWORDS if c.lower() in text.lower()]
            orgs = []
            for pattern in self.org_patterns:
                orgs.extend(pattern.findall(text))
            e["entities"] = {
                "countries": list(set(countries)),
                "organizations": list(set(orgs)),
                "total": len(set(countries)) + len(set(orgs)),
            }
        event.data["entity_extracted_events"] = events
        return event


class EntityExtractionPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="nlp_entities",
            stages=[EntityExtractionStage()],
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
