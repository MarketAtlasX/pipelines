from __future__ import annotations

import logging
import tempfile
from typing import Any, Dict, List, Optional

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class ModelTrainingStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("model_training")

    async def run(self, event: Event, context: Context) -> Event:
        features = event.data.get("features", [])
        if len(features) < 10:
            logger.warning("Too few samples for training: %d", len(features))
            event.data["training_status"] = "skipped"
            return event

        X = np.array([
            [f.get("sentiment_score", 0), f.get("entity_count", 0), f.get("text_length", 0)]
            for f in features
        ])
        y = np.array([
            1 if f.get("sentiment_label") == "positive" else (0 if f.get("sentiment_label") == "negative" else 2)
            for f in features
        ])

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)
        model.fit(X_train, y_train)

        train_acc = model.score(X_train, y_train)
        test_acc = model.score(X_test, y_test)

        event.data["model"] = model
        event.data["training_metrics"] = {
            "train_accuracy": round(train_acc, 4),
            "test_accuracy": round(test_acc, 4),
            "train_samples": len(X_train),
            "test_samples": len(X_test),
            "feature_dim": X.shape[1],
        }

        logger.info("Model trained: train_acc=%.4f, test_acc=%.4f", train_acc, test_acc)
        return event


class TrainingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="training",
            stages=[ModelTrainingStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            metrics = result.data.get("training_metrics", {})
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics=metrics,
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
