from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class SimilarityEmbeddingStage(PipelineStage):
    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        super().__init__("similarity_embedding")
        self.model_name = model_name
        self._model = None

    async def _load_model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
            logger.info("Similarity: loaded model %s", self.model_name)

    async def run(self, event: Event, context: Context) -> Event:
        await self._load_model()
        text = f"{event.data.get('title', '')} {event.data.get('content', '')}"
        embedding = self._model.encode(text).tolist()
        event.data["query_embedding"] = embedding
        event.data["query_text"] = text
        logger.debug("Similarity embedding generated for event %s", event.id[:8])
        return event


class SimilarityEmbeddingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="similarity_embedding",
            stages=[SimilarityEmbeddingStage()],
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
