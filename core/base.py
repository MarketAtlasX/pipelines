from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Callable, List, Optional

from pipelines.core.state import PipelineState
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class PipelineStage(ABC):
    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    async def run(self, event: Event, context: Context) -> Event: ...

    async def __call__(self, event: Event, context: Context) -> Event:
        logger.debug("Stage %s processing event %s", self.name, event.id)
        result = await self.run(event, context)
        return result


class Pipeline(ABC):
    name: str
    stages: List[PipelineStage]
    state: PipelineState

    def __init__(self, name: str, stages: Optional[List[PipelineStage]] = None) -> None:
        self.name = name
        self.stages = stages or []
        self.state = PipelineState(name)

    def add_stage(self, stage: PipelineStage) -> Pipeline:
        self.stages.append(stage)
        return self

    @abstractmethod
    async def run(self, event: Event, context: Context) -> Outcome: ...

    async def execute(self, event: Event, context: Context) -> Event:
        current = event
        for stage in self.stages:
            current = await stage(current, context)
        return current

    def on_complete(self, handler: Callable[[Outcome], Any]) -> None:
        self.state.add_handler("complete", handler)

    def on_failure(self, handler: Callable[[Outcome], Any]) -> None:
        self.state.add_handler("failure", handler)

    async def __call__(self, event: Event, context: Optional[Context] = None) -> Outcome:
        ctx = context or Context(pipeline=self.name, pipeline_type=event.type)  # type: ignore
        return await self.run(event if event is not None else event, ctx)
