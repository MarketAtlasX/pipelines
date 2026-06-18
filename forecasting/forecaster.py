from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class ForecastStage(PipelineStage):
    def __init__(self, horizon_days: int = 30) -> None:
        super().__init__("forecast")
        self.horizon_days = horizon_days

    async def run(self, event: Event, context: Context) -> Event:
        aggregates = event.data.get("feature_aggregates", {})
        current_sentiment = aggregates.get("avg_sentiment", 0)
        signals = event.data.get("signals", [])

        base = current_sentiment
        momentum = 0.0

        for s in signals:
            if s.get("direction") == "bearish":
                momentum -= 0.1 * s.get("confidence", 0.5)
            elif s.get("direction") == "bullish":
                momentum += 0.1 * s.get("confidence", 0.5)

        forecast = []
        drift = momentum * 0.05
        for day in range(1, self.horizon_days + 1):
            noise = np.random.normal(0, 0.02)
            value = base + drift * day + noise
            forecast.append({
                "day": day,
                "date": (datetime.utcnow() + timedelta(days=day)).date().isoformat(),
                "predicted_sentiment": round(float(value), 4),
                "lower_bound": round(float(value - 0.1), 4),
                "upper_bound": round(float(value + 0.1), 4),
            })

        trend = "bullish" if drift > 0.01 else ("bearish" if drift < -0.01 else "neutral")

        event.data["forecast"] = {
            "horizon_days": self.horizon_days,
            "trend": trend,
            "confidence": float(abs(drift) * 10 + 0.5),
            "projections": forecast,
            "summary": {
                "start": current_sentiment,
                "end": forecast[-1]["predicted_sentiment"],
                "change": round(forecast[-1]["predicted_sentiment"] - current_sentiment, 4),
            },
        }

        logger.info("Forecast generated: %s trend over %d days", trend, self.horizon_days)
        return event


class ForecastingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="forecasting",
            stages=[ForecastStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            fcast = result.data.get("forecast", {})
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={
                    "trend": 1 if fcast.get("trend") == "bullish" else (-1 if fcast.get("trend") == "bearish" else 0),
                    "confidence": fcast.get("confidence", 0),
                },
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
