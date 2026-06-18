from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

ECONOMIC_INDICATORS = {
    "gdp": ["GDP", "gross domestic product", "economic growth"],
    "inflation": ["CPI", "inflation", "consumer price", "price index"],
    "unemployment": ["unemployment", "jobless", "labor market", "employment"],
    "interest_rates": ["interest rate", "fed rate", "central bank", "monetary policy"],
    "trade": ["trade deficit", "trade surplus", "tariff", "export", "import"],
    "fiscal": ["deficit", "debt", "budget", "fiscal policy", "stimulus"],
    "manufacturing": ["PMI", "manufacturing", "industrial production"],
    "consumer": ["consumer confidence", "retail sales", "consumer spending"],
}


class EconomicDataStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("economic_data")

    async def run(self, event: Event, context: Context) -> Event:
        text = f"{event.data.get('title', '')} {event.data.get('content', '')}"
        indicators = {}
        for indicator, keywords in ECONOMIC_INDICATORS.items():
            matches = [kw for kw in keywords if kw.lower() in text.lower()]
            if matches:
                indicators[indicator] = {"matched": matches, "count": len(matches)}

        event.data["economic_intel"] = {
            "indicators_detected": indicators,
            "indicator_count": len(indicators),
            "primary_sectors": self._detect_sectors(text),
        }
        logger.info("Economic analysis: %d indicators detected", len(indicators))
        return event

    def _detect_sectors(self, text: str) -> List[str]:
        sectors = []
        mappings = {
            "energy": ["oil", "gas", "energy", "petroleum", "renewable"],
            "technology": ["tech", "AI", "semiconductor", "software", "cyber"],
            "finance": ["bank", "finance", "insurance", "fintech", "lending"],
            "healthcare": ["health", "pharma", "biotech", "medical"],
            "defense": ["defense", "military", "aerospace", "arms"],
            "agriculture": ["agriculture", "farming", "crop", "food"],
        }
        for sector, kws in mappings.items():
            if any(kw.lower() in text.lower() for kw in kws):
                sectors.append(sector)
        return sectors


class EconomicIntelligencePipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="economic_intelligence",
            stages=[EconomicDataStage()],
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
