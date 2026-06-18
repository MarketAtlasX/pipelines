from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class StreamTransformStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("stream_transform")

    async def run(self, event: Event, context: Context) -> Event:
        event.data["stream_processed"] = True
        event.data["processing_latency_ms"] = 0
        logger.debug("Stream processed event %s", event.id[:8])
        return event


class StreamFilterStage(PipelineStage):
    def __init__(self, min_confidence: float = 0.3) -> None:
        super().__init__("stream_filter")
        self.min_confidence = min_confidence

    async def run(self, event: Event, context: Context) -> Event:
        signals = event.data.get("signals", [])
        filtered = [s for s in signals if s.get("confidence", 0) >= self.min_confidence]
        event.data["signals"] = filtered
        event.data["filtered_signal_count"] = len(filtered)
        return event


class StreamProcessorPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="stream_processor",
            stages=[
                StreamTransformStage(),
                StreamFilterStage(),
            ],
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
