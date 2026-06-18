from __future__ import annotations

import logging
from typing import Any, Dict, List

import numpy as np

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class RiskMetricsStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("risk_metrics")

    async def run(self, event: Event, context: Context) -> Event:
        backtest = event.data.get("backtest", {})
        trades = backtest.get("trades", [])
        forecast = event.data.get("forecast", {}).get("projections", [])

        returns = self._compute_returns(trades)
        metrics = {
            "sharpe_ratio": self._sharpe_ratio(returns),
            "max_drawdown": backtest.get("max_drawdown_pct", 0),
            "total_return": backtest.get("total_return_pct", 0),
            "win_rate": self._win_rate(trades),
            "avg_return_per_trade": self._avg_return(trades),
        }

        event.data["performance_metrics"] = metrics
        logger.info("Risk metrics: Sharpe=%.2f, WinRate=%.0f%%", metrics["sharpe_ratio"], metrics["win_rate"] * 100)
        return event

    def _compute_returns(self, trades: List[Dict]) -> List[float]:
        rets = []
        for i in range(1, len(trades)):
            prev = trades[i - 1].get("value", trades[i - 1].get("price", 0))
            curr = trades[i].get("value", trades[i].get("price", 0))
            if prev and curr:
                rets.append((curr - prev) / prev)
        return rets or [0.0]

    def _sharpe_ratio(self, returns: List[float], risk_free: float = 0.02) -> float:
        if len(returns) < 2:
            return 0.0
        excess = np.mean(returns) - risk_free / 252
        std = np.std(returns)
        return float(np.sqrt(252) * excess / std) if std > 0 else 0.0

    def _win_rate(self, trades: List[Dict]) -> float:
        wins = 0
        total = 0
        for t in trades:
            if "value" in t:
                total += 1
                if t.get("value", 0) > 0:
                    wins += 1
        return wins / max(total, 1)

    def _avg_return(self, trades: List[Dict]) -> float:
        vals = [t.get("value", 0) for t in trades if "value" in t]
        if not vals:
            return 0.0
        init = trades[0].get("price", 1) if trades else 1
        return float(np.mean(vals) / init - 1)


class MetricsPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="backtest_metrics",
            stages=[RiskMetricsStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            metrics = result.data.get("performance_metrics", {})
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics=metrics,
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
