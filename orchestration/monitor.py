from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipelines.core.types import Outcome, PipelineStatus

logger = logging.getLogger(__name__)


@dataclass
class PipelineRun:
    run_id: str
    pipeline_name: str
    status: PipelineStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    stages: List[Dict[str, Any]] = field(default_factory=list)


class Monitor:
    def __init__(self) -> None:
        self._runs: Dict[str, PipelineRun] = {}

    def record(self, outcome: Outcome) -> PipelineRun:
        run = PipelineRun(
            run_id=outcome.context.run_id,
            pipeline_name=outcome.context.pipeline,
            status=outcome.status,
            started_at=outcome.context.started_at,
            completed_at=outcome.completed_at,
            error=outcome.error,
            metrics=outcome.metrics,
        )
        if run.started_at and run.completed_at:
            run.duration_ms = (
                run.completed_at - run.started_at
            ).total_seconds() * 1000
        self._runs[run.run_id] = run

        status_icon = "✓" if run.status == PipelineStatus.SUCCESS else "✗"
        logger.info(
            "%s %s (%s) — %.0fms",
            status_icon,
            run.pipeline_name,
            run.status.value,
            run.duration_ms or 0,
        )
        return run

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self._runs.get(run_id)

    def recent_runs(self, n: int = 10) -> List[PipelineRun]:
        return sorted(
            self._runs.values(), key=lambda r: r.started_at, reverse=True
        )[:n]

    def failures(self) -> List[PipelineRun]:
        return [r for r in self._runs.values() if r.status == PipelineStatus.FAILED]

    def summary(self) -> Dict[str, Any]:
        total = len(self._runs)
        failed = len(self.failures())
        return {
            "total_runs": total,
            "successful": total - failed,
            "failed": failed,
            "success_rate": ((total - failed) / total * 100) if total else 0,
        }
