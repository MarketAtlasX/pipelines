from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from pipelines.core.base import Pipeline
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class PipelineExecutor:
    def __init__(self, max_workers: int = 4) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._active: Dict[str, asyncio.Task] = {}

    async def run(
        self,
        pipeline: Pipeline,
        event: Event,
        context: Optional[Context] = None,
    ) -> Outcome:
        ctx = context or Context(
            pipeline=pipeline.name,
            pipeline_type=pipeline.name,  # type: ignore
        )
        pipeline.state.start()
        try:
            outcome = await pipeline.run(event, ctx)
            pipeline.state.succeed(outcome)
            return outcome
        except Exception as e:
            logger.exception("Pipeline '%s' execution failed", pipeline.name)
            outcome = Outcome(
                context=ctx,
                status=PipelineStatus.FAILED,
                error=str(e),
            )
            pipeline.state.fail(outcome)
            return outcome

    async def run_many(
        self,
        pipelines: List[Pipeline],
        event: Event,
        context: Optional[Context] = None,
    ) -> List[Outcome]:
        tasks = [self.run(p, event, context) for p in pipelines]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def cancel(self, pipeline_name: str) -> bool:
        task = self._active.get(pipeline_name)
        if task and not task.done():
            task.cancel()
            return True
        return False
