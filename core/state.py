from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pipelines.core.types import Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class PipelineState:
    def __init__(self, pipeline_name: str) -> None:
        self.pipeline_name = pipeline_name
        self.status: PipelineStatus = PipelineStatus.PENDING
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.current_stage: Optional[str] = None
        self.error: Optional[str] = None
        self.metrics: Dict[str, float] = {}
        self._handlers: Dict[str, List[Callable]] = {"complete": [], "failure": []}

    def start(self) -> None:
        self.status = PipelineStatus.RUNNING
        self.started_at = datetime.utcnow()
        logger.info("Pipeline %s started", self.pipeline_name)

    def succeed(self, outcome: Outcome) -> None:
        self.status = PipelineStatus.SUCCESS
        self.completed_at = datetime.utcnow()
        self.metrics.update(outcome.metrics)
        logger.info("Pipeline %s completed in %s", self.pipeline_name, self.elapsed)
        for handler in self._handlers["complete"]:
            handler(outcome)

    def fail(self, outcome: Outcome) -> None:
        self.status = PipelineStatus.FAILED
        self.completed_at = datetime.utcnow()
        self.error = outcome.error
        logger.error("Pipeline %s failed: %s", self.pipeline_name, self.error)
        for handler in self._handlers["failure"]:
            handler(outcome)

    def stage_progress(self, stage_name: str) -> None:
        self.current_stage = stage_name
        logger.debug("Pipeline %s at stage %s", self.pipeline_name, stage_name)

    @property
    def elapsed(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def add_handler(self, event: str, handler: Callable) -> None:
        self._handlers.setdefault(event, []).append(handler)
