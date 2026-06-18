from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

MONITORED_AIRSPACE = [
    "Ukraine", "Russia", "Belarus", "Iran", "Iraq", "Syria",
    "Afghanistan", "North Korea", "Myanmar", "Israel", "Gaza",
    "Taiwan Strait", "South China Sea",
]


class FlightDataStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("flight_data")

    async def run(self, event: Event, context: Context) -> Event:
        event.data["flight_intel"] = {
            "source": "ADS-B_Flight_Tracking",
            "tracked_regions": MONITORED_AIRSPACE,
            "active_flights": 0,
            "airspace_restrictions": [],
            "military_movements": [],
            "status": "simulated",
        }
        logger.info("Flight pipeline: monitoring %d regions", len(MONITORED_AIRSPACE))
        return event


class FlightAnalysisStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("flight_analysis")

    async def run(self, event: Event, context: Context) -> Event:
        fl = event.data.get("flight_intel", {})
        fl["analysis"] = {
            "unusual_routes": False,
            "airspace_closures": [],
            "military_activity_level": "low",
            "commercial_traffic_normal": True,
        }
        fl["last_updated"] = datetime.utcnow().isoformat()
        event.data["flight_intel"] = fl
        return event


class FlightTrackingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="alternative_flights",
            stages=[FlightDataStage(), FlightAnalysisStage()],
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
