from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class ModelRegistryStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("model_registry")
        self._registry: Dict[str, Dict[str, Any]] = {}

    async def run(self, event: Event, context: Context) -> Event:
        action = context.params.get("action", "register")

        if action == "register":
            model = event.data.get("model")
            if model:
                entry = {
                    "model_id": uuid.uuid4().hex,
                    "registered_at": datetime.utcnow().isoformat(),
                    "metrics": event.data.get("training_metrics", {}),
                    "params": context.params.get("model_params", {}),
                }
                self._registry[f"model_{len(self._registry) + 1}"] = entry
                event.data["registry_entry"] = entry
                logger.info("Registered model: %s", entry["model_id"])

        elif action == "list":
            event.data["registered_models"] = list(self._registry.values())

        elif action == "get":
            model_id = context.params.get("model_id")
            for entry in self._registry.values():
                if entry["model_id"] == model_id:
                    event.data["model_entry"] = entry
                    break

        return event


class ModelRegistryPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="model_registry",
            stages=[ModelRegistryStage()],
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
