from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class ReportGenerationStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("report_generation")

    async def run(self, event: Event, context: Context) -> Event:
        backtest = event.data.get("backtest", {})
        metrics = event.data.get("performance_metrics", {})
        forecast = event.data.get("forecast", {})

        report = {
            "report_id": f"bt_{event.id[:8]}",
            "generated_at": datetime.utcnow().isoformat(),
            "pipeline_run": context.run_id,
            "summary": {
                "total_return_pct": backtest.get("total_return_pct", 0),
                "max_drawdown_pct": backtest.get("max_drawdown_pct", 0),
                "trade_count": backtest.get("trade_count", 0),
                "sharpe_ratio": metrics.get("sharpe_ratio", 0),
                "win_rate": f"{metrics.get('win_rate', 0) * 100:.1f}%",
            },
            "backtest_details": backtest,
            "metrics": metrics,
            "forecast_summary": {
                "trend": forecast.get("trend", "unknown"),
                "horizon": forecast.get("horizon_days", 0),
            },
        }

        event.data["report"] = report
        logger.info("Report generated: %s", report["report_id"])
        return event


class ReportingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="backtest_reporting",
            stages=[ReportGenerationStage()],
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
