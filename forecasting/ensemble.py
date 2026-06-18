from __future__ import annotations

import logging
from typing import Any, Dict, List

import numpy as np

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class EnsembleStage(PipelineStage):
    def __init__(self, weights: Optional[List[float]] = None) -> None:
        super().__init__("ensemble")
        self.weights = weights or [0.5, 0.3, 0.2]

    async def run(self, event: Event, context: Context) -> Event:
        forecasts = [
            event.data.get("prophet_forecast", {}).get("predictions", []),
            event.data.get("lstm_forecast", {}).get("predictions", []),
            event.data.get("forecast", {}).get("projections", []),
        ]
        valid = [f for f in forecasts if len(f) > 0]

        if not valid:
            logger.warning("No forecasts to ensemble")
            return event

        min_len = min(len(f) for f in valid)
        weights = self.weights[:len(valid)]

        ensemble = np.average(
            [f[:min_len] for f in valid],
            axis=0,
            weights=weights[:len(valid)],
        )

        event.data["ensemble_forecast"] = {
            "predictions": [float(v) for v in ensemble],
            "weights": weights,
            "models_used": len(valid),
        }

        logger.info("Ensemble forecast generated from %d models", len(valid))
        return event


class EnsemblePipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="forecast_ensemble",
            stages=[EnsembleStage()],
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
