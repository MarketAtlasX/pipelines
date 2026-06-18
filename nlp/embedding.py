from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class EmbeddingStage(PipelineStage):
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", device: str = "cpu", batch_size: int = 64) -> None:
        super().__init__("nlp_embedding")
        self.model_name = model_name
        self.device = device
        self.batch_size = batch_size
        self._model = None

    async def _load_model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name, device=self.device)
            logger.info("Loaded embedding model: %s", self.model_name)

    async def run(self, event: Event, context: Context) -> Event:
        await self._load_model()
        events_data = event.data.get("cleaned_events") or event.data.get("transformed_events") or [event.model_dump()]
        texts = []
        for e in events_data:
            if isinstance(e, dict):
                texts.append(f"{e.get('title', '')} {e.get('content', '')}")
            else:
                texts.append(str(e))

        embeddings = self._model.encode(texts, batch_size=self.batch_size, show_progress_bar=False)
        embedded = []
        for i, e in enumerate(events_data):
            if isinstance(e, dict):
                e["embedding"] = embeddings[i].tolist()
                embedded.append(e)
        event.data["embedded_events"] = embedded
        event.data["embedding_count"] = len(embedded)
        event.data["embedding_dim"] = embeddings.shape[1]
        logger.info("Generated %d embeddings (dim=%d)", len(embedded), embeddings.shape[1])
        return event


class EmbeddingPipeline(Pipeline):
    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        super().__init__(
            name="nlp_embedding",
            stages=[EmbeddingStage(model_name=model_name)],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"embeddings": result.data.get("embedding_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
