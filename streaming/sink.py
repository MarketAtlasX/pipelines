from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.store import PostgresStore, RedisStore

logger = logging.getLogger(__name__)


class StreamSinkStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("stream_sink")
        self.stores = [RedisStore("redis://localhost:6379")]

    async def run(self, event: Event, context: Context) -> Event:
        for store in self.stores:
            await store.write(event)
        event.data["sink_complete"] = True
        event.data["sink_count"] = len(self.stores)
        return event


class StreamSinkPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="streaming_sink",
            stages=[StreamSinkStage()],
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
