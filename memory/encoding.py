from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

MEMORY_URL = os.getenv("MEMORY_URL", "http://localhost:8010")


class MemoryEncodingStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("memory_encoding")

    async def run(self, event: Event, context: Context) -> Event:
        event_data = event.data

        articles = event_data.get("articles") or event_data.get("summarized_events") or event_data.get("cleaned_events") or [event_data]
        if not isinstance(articles, list):
            articles = [articles]

        normalized = []
        for article in articles:
            if isinstance(article, dict):
                normalized.append({
                    "title": article.get("title", article.get("event_type", "Pipeline Event")),
                    "summary": article.get("summary", article.get("description", "")),
                    "locations": article.get("locations", []),
                    "sectors": article.get("sectors", article.get("affected_sectors", [])),
                    "entities": article.get("entities", []),
                    "source": "pipeline",
                    "pipeline_name": context.pipeline,
                })

        if normalized:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(
                        f"{MEMORY_URL}/api/v1/memory/episodes",
                        json=normalized,
                    )
                    resp.raise_for_status()
                    result = resp.json()
                    logger.info("Memory encoding complete: %s", result.get("id", "unknown"))
                    event.data["memory_episode_id"] = result.get("id")
            except httpx.TimeoutException:
                logger.warning("Memory service timed out during encoding")
            except httpx.HTTPStatusError as e:
                logger.warning("Memory service returned %s: %s", e.response.status_code, e.response.text)
            except httpx.RequestError as e:
                logger.warning("Memory service unreachable: %s", e)
        else:
            logger.info("No articles to encode in memory")

        return event


class MemoryEncodingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="memory_encoding",
            stages=[MemoryEncodingStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result_event = await self.execute(event, context)
            outcome = Outcome(
                status=PipelineStatus.SUCCESS,
                event=result_event,
                metrics={"memory_episode_id": result_event.data.get("memory_episode_id")},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            logger.exception("Memory encoding pipeline failed: %s", e)
            outcome = Outcome(
                status=PipelineStatus.FAILED,
                event=event,
                error=str(e),
            )
            self.state.fail(outcome)
            return outcome
