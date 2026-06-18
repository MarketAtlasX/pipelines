from __future__ import annotations

import logging

from pipelines.core.base import PipelineStage
from pipelines.core.types import Context, Event

logger = logging.getLogger(__name__)


class DailySignalStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("daily_signals")

    async def run(self, event: Event, context: Context) -> Event:
        aggregates = event.data.get("daily_feature_aggregates", {})
        features = event.data.get("features", [])
        signals = []

        avg_sent = aggregates.get("avg_sentiment", 0)

        if avg_sent < -0.3:
            signals.append({
                "type": "market_risk",
                "severity": "high",
                "direction": "bearish",
                "confidence": abs(avg_sent),
                "reason": f"Daily aggregate sentiment {avg_sent:.2f} indicates elevated risk",
            })
        elif avg_sent < -0.1:
            signals.append({
                "type": "market_caution",
                "severity": "medium",
                "direction": "bearish",
                "confidence": abs(avg_sent),
                "reason": f"Daily aggregate sentiment {avg_sent:.2f} suggests caution",
            })

        if avg_sent > 0.3:
            signals.append({
                "type": "market_opportunity",
                "severity": "medium",
                "direction": "bullish",
                "confidence": avg_sent,
                "reason": f"Daily aggregate sentiment {avg_sent:.2f} indicates opportunity",
            })

        if aggregates.get("avg_entities", 0) > 3:
            signals.append({
                "type": "high_entity_density",
                "severity": "low",
                "direction": "neutral",
                "confidence": min(aggregates.get("avg_entities", 0) / 10, 1.0),
                "reason": "High entity density suggests complex multi-actor event",
            })

        event.data["signals"] = signals
        event.data["signal_count"] = len(signals)
        logger.info("Daily signal generation: %d signals", len(signals))
        return event
