from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
from sklearn.metrics import classification_report, confusion_matrix

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class ModelEvaluationStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("model_evaluation")

    async def run(self, event: Event, context: Context) -> Event:
        model = event.data.get("model")
        features = event.data.get("features", [])

        if model is None or len(features) < 5:
            logger.warning("Cannot evaluate: missing model or insufficient features")
            event.data["evaluation_status"] = "skipped"
            return event

        X = np.array([
            [f.get("sentiment_score", 0), f.get("entity_count", 0), f.get("text_length", 0)]
            for f in features
        ])
        y_true = np.array([
            1 if f.get("sentiment_label") == "positive" else (0 if f.get("sentiment_label") == "negative" else 2)
            for f in features
        ])

        y_pred = model.predict(X)

        report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
        cm = confusion_matrix(y_true, y_pred).tolist()

        event.data["evaluation"] = {
            "classification_report": report,
            "confusion_matrix": cm,
            "accuracy": round(model.score(X, y_true), 4),
        }

        logger.info("Evaluation complete: accuracy=%.4f", event.data["evaluation"]["accuracy"])
        return event


class EvaluationPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="model_evaluation",
            stages=[ModelEvaluationStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            eval_data = result.data.get("evaluation", {})
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"accuracy": eval_data.get("accuracy", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
