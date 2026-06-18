from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

SIGNAL_THRESHOLDS = {
    "strong_bearish": -0.5,
    "bearish": -0.2,
    "neutral": 0.2,
    "bullish": 0.5,
    "strong_bullish": 1.0,
}


class SignalGenerationStage(PipelineStage):
    def __init__(self, window_days: int = 7) -> None:
        super().__init__("signal_generation")
        self.window_days = window_days

    async def run(self, event: Event, context: Context) -> Event:
        aggregates = event.data.get("feature_aggregates", {})
        features = event.data.get("features", [])
        signals = []

        avg_sentiment = aggregates.get("avg_sentiment", 0)
        negative_ratio = aggregates.get("negative_ratio", 0)
        positive_ratio = aggregates.get("positive_ratio", 0)

        if avg_sentiment <= SIGNAL_THRESHOLDS["strong_bearish"]:
            signals.append({
                "type": "market_risk",
                "severity": "high",
                "direction": "bearish",
                "confidence": abs(avg_sentiment),
                "reason": f"Strong negative sentiment ({avg_sentiment:.2f})",
            })
        elif avg_sentiment <= SIGNAL_THRESHOLDS["bearish"]:
            signals.append({
                "type": "market_risk",
                "severity": "medium",
                "direction": "bearish",
                "confidence": abs(avg_sentiment),
                "reason": f"Negative sentiment ({avg_sentiment:.2f})",
            })

        if avg_sentiment >= SIGNAL_THRESHOLDS["strong_bullish"]:
            signals.append({
                "type": "market_opportunity",
                "severity": "high",
                "direction": "bullish",
                "confidence": avg_sentiment,
                "reason": f"Strong positive sentiment ({avg_sentiment:.2f})",
            })

        if negative_ratio > 0.6:
            signals.append({
                "type": "negative_surge",
                "severity": "high",
                "direction": "bearish",
                "confidence": negative_ratio,
                "reason": f"Negative ratio {negative_ratio:.0%} exceeds threshold",
            })

        if aggregates.get("geo_ratio", 0) > 0.5:
            signals.append({
                "type": "geopolitical_activity",
                "severity": "medium",
                "direction": "neutral",
                "confidence": aggregates["geo_ratio"],
                "reason": "High geographic diversity in events",
            })

        event.data["signals"] = signals
        event.data["signal_count"] = len(signals)
        logger.info("Generated %d signals", len(signals))
        return event


class SignalGenerationPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="signal_generation",
            stages=[SignalGenerationStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"signals": result.data.get("signal_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
