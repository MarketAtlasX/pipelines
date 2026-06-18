from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

MONITORED_SITES = [
    {"name": "Nuclear Facility - Bushehr", "country": "Iran", "lat": 28.83, "lon": 50.89},
    {"name": "Nuclear Facility - Yongbyon", "country": "North Korea", "lat": 39.79, "lon": 125.75},
    {"name": "Military Base - Tartus", "country": "Syria", "lat": 34.89, "lon": 35.88},
    {"name": "Port - Shanghai", "country": "China", "lat": 31.23, "lon": 121.47},
    {"name": "Military Base - Crimea", "country": "Ukraine", "lat": 44.95, "lon": 34.10},
]


class SatelliteDataStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("satellite_data")

    async def run(self, event: Event, context: Context) -> Event:
        event.data["satellite_intel"] = {
            "source": "Satellite_Imagery",
            "monitored_sites": MONITORED_SITES,
            "image_count": 0,
            "detected_changes": [],
            "coverage_area": "global",
            "status": "simulated",
        }
        logger.info("Satellite pipeline: monitoring %d sites", len(MONITORED_SITES))
        return event


class SatelliteAnalysisStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("satellite_analysis")

    async def run(self, event: Event, context: Context) -> Event:
        sat = event.data.get("satellite_intel", {})
        sat["analysis"] = {
            "construction_activity": False,
            "troop_movement_detected": False,
            "vessel_activity": "normal",
            "deforestation": False,
        }
        sat["last_updated"] = datetime.utcnow().isoformat()
        event.data["satellite_intel"] = sat
        return event


class SatelliteImageryPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="alternative_satellite",
            stages=[SatelliteDataStage(), SatelliteAnalysisStage()],
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
