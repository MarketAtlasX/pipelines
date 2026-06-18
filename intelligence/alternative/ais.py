from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

CHOKEPOINTS = [
    "Strait of Hormuz", "Strait of Malacca", "Suez Canal", "Panama Canal",
    "Bab-el-Mandeb", "Turkish Straits", "South China Sea", "Baltic Sea",
]


class AISDataStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("ais_data")

    async def run(self, event: Event, context: Context) -> Event:
        event.data["ais_intel"] = {
            "source": "AIS_Vessel_Tracking",
            "vessel_count": 0,
            "chokepoints_monitored": CHOKEPOINTS,
            "anomaly_detected": False,
            "region": context.params.get("region", "global"),
            "status": "simulated",
        }
        logger.info("AIS pipeline: monitoring %d chokepoints", len(CHOKEPOINTS))
        return event


class AISAnomalyStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("ais_anomaly")

    async def run(self, event: Event, context: Context) -> Event:
        ais = event.data.get("ais_intel", {})
        ais["anomaly_analysis"] = {
            "unusual_traffic": False,
            "dark_ships": 0,
            "port_congestion": "normal",
            "piracy_risk": "low",
            "chokepoint_status": {cp: "normal" for cp in CHOKEPOINTS},
        }
        ais["last_updated"] = datetime.utcnow().isoformat()
        event.data["ais_intel"] = ais
        return event


class AISVesselTrackingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="alternative_ais",
            stages=[AISDataStage(), AISAnomalyStage()],
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
