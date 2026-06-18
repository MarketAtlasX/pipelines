from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class ProphetModelStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("prophet_forecast")

    async def run(self, event: Event, context: Context) -> Event:
        signal_history = event.data.get("signal_history", [])
        values = [s.get("value", 0) for s in signal_history] or [0.0]
        t = np.arange(len(values))
        coeffs = np.polyfit(t, values, 1)
        trend = coeffs[0]

        predictions = [float(values[-1] + trend * i) for i in range(1, 31)]
        event.data["prophet_forecast"] = {
            "trend": float(trend),
            "predictions": predictions,
            "model": "linear_trend",
        }
        return event


class LSTMModelStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("lstm_forecast")

    async def run(self, event: Event, context: Context) -> Event:
        signal_history = event.data.get("signal_history", [])
        values = [s.get("value", 0) for s in signal_history] or [0.0]
        n = len(values)
        if n > 0:
            last = values[-1]
            noise = np.random.normal(0, 0.01, 30)
            predictions = [float(last * (1 + 0.001 * i) + noise[i]) for i in range(30)]
        else:
            predictions = [0.0] * 30
        event.data["lstm_forecast"] = {
            "predictions": predictions,
            "model": "lstm_simulated",
        }
        return event


class ForecastModelsPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="forecast_models",
            stages=[
                ProphetModelStage(),
                LSTMModelStage(),
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
