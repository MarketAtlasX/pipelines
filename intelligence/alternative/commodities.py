from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

COMMODITY_CATEGORIES = {
    "energy": ["crude oil", "natural gas", "gasoline", "heating oil", "diesel"],
    "metals": ["gold", "silver", "copper", "platinum", "palladium", "aluminum", "steel", "lithium"],
    "agriculture": ["wheat", "corn", "soybeans", "coffee", "sugar", "cotton", "rice", "palm oil"],
    "softs": ["cocoa", "orange juice", "lumber", "rubber"],
}


class CommodityDataStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("commodity_data")

    async def run(self, event: Event, context: Context) -> Event:
        event.data["commodity_intel"] = {
            "source": "Commodity_Flow_Tracking",
            "categories": list(COMMODITY_CATEGORIES.keys()),
            "tracked_commodities": [c for cat in COMMODITY_CATEGORIES.values() for c in cat],
            "flow_anomalies": [],
            "supply_disruptions": [],
            "status": "simulated",
        }
        logger.info("Commodity pipeline tracking %d commodities")
        return event


class CommodityAnalysisStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("commodity_analysis")

    async def run(self, event: Event, context: Context) -> Event:
        cm = event.data.get("commodity_intel", {})
        cm["analysis"] = {
            "price_momentum": "neutral",
            "supply_chain_risk": "low",
            "critical_bottlenecks": [],
            "strategic_reserves_status": "adequate",
        }
        cm["last_updated"] = datetime.utcnow().isoformat()

        text = f"{event.data.get('title', '')} {event.data.get('content', '')}"
        for category, commodities in COMMODITY_CATEGORIES.items():
            mentioned = [c for c in commodities if c.lower() in text.lower()]
            if mentioned:
                cm["analysis"][f"{category}_mentioned"] = mentioned

        event.data["commodity_intel"] = cm
        return event


class CommodityFlowsPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="alternative_commodities",
            stages=[CommodityDataStage(), CommodityAnalysisStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(context=context, status=PipelineStatus.SUCCESS, events=[result])
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
