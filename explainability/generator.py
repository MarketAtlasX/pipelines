from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class ExplanationGenerationStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("explanation_generation")

    async def run(self, event: Event, context: Context) -> Event:
        shap_data = event.data.get("shap_explanation", {})
        graph_paths = event.data.get("graph_paths", [])
        analogs = event.data.get("historical_analogs", [])
        signals = event.data.get("signals", [])
        forecast = event.data.get("forecast", {})

        top_feature = shap_data.get("top_feature", {"feature": "unknown"})
        top_analog = analogs[0] if analogs else {"name": "None"}
        top_signal = signals[0] if signals else {"reason": "No strong signals"}
        trend = forecast.get("trend", "neutral")

        explanation = {
            "generated_at": datetime.utcnow().isoformat(),
            "event_id": event.id,
            "summary": (
                f"The {trend} prediction is primarily driven by {top_feature['feature']} "
                f"(SHAP value: {top_feature.get('shap_value', 0):.3f}). "
                f"This pattern resembles the {top_analog['name']} event "
                f"(similarity: {top_analog.get('similarity_score', 0):.2f}). "
                f"Key signal: {top_signal.get('reason', 'N/A')}. "
                f"{len(graph_paths)} causal paths identified in the knowledge graph."
            ),
            "key_factors": [f["feature"] for f in shap_data.get("features", [])],
            "analog_event": top_analog.get("name"),
            "analog_similarity": top_analog.get("similarity_score", 0),
            "causal_paths": len(graph_paths),
            "active_signals": [s.get("reason") for s in signals[:3]],
            "confidence": forecast.get("confidence", 0.5),
            "recommendation": self._generate_recommendation(trend, signals, analogs),
        }

        event.data["explanation"] = explanation
        logger.info("Explanation generated for event %s", event.id[:8])
        return event

    def _generate_recommendation(self, trend: str, signals: List[Dict], analogs: List[Dict]) -> str:
        if trend == "bearish" and any(s.get("severity") == "high" for s in signals):
            return "Increase hedging positions. Monitor analog events for historical drawdown patterns."
        elif trend == "bullish":
            return "Consider increasing exposure. Historical analogs suggest sustained positive momentum."
        return "Maintain current positioning. No strong directional signals detected."


class ExplanationGeneratorPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="explainability_generator",
            stages=[ExplanationGenerationStage()],
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
