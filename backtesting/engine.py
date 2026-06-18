from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class BacktestStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("backtest")

    async def run(self, event: Event, context: Context) -> Event:
        forecast = event.data.get("forecast", {}).get("projections", [])
        signals = event.data.get("signals", [])

        actuals = self._generate_actuals(forecast)
        positions = self._generate_positions(signals, len(forecast))
        portfolio = self._simulate_trades(positions, actuals)

        event.data["backtest"] = portfolio
        logger.info("Backtest complete: %d trades, return=%.2f%%", portfolio["trade_count"], portfolio["total_return_pct"])
        return event

    def _generate_actuals(self, forecast: List[Dict]) -> List[float]:
        base = forecast[0]["predicted_sentiment"] if forecast else 0
        np.random.seed(42)
        return [float(base * (1 + np.random.normal(0, 0.03))) for _ in range(len(forecast) or 30)]

    def _generate_positions(self, signals: List[Dict], days: int) -> List[int]:
        if not days:
            return []
        avg_direction = 0
        for s in signals:
            if s.get("direction") == "bullish":
                avg_direction += 1
            elif s.get("direction") == "bearish":
                avg_direction -= 1
        bias = 1 if avg_direction > 0 else (-1 if avg_direction < 0 else 0)
        np.random.seed(42)
        return [bias if np.random.random() > 0.3 else 0 for _ in range(days)]

    def _simulate_trades(self, positions: List[int], actuals: List[float]) -> Dict[str, Any]:
        capital = 10000.0
        holdings = 0
        trades = []
        peak = capital

        for i, (pos, act) in enumerate(zip(positions, actuals)):
            if pos != 0 and holdings == 0:
                holdings = capital / max(abs(act), 0.001)
                capital = 0
                trades.append({"day": i, "action": "buy", "price": act, "units": holdings})
            elif pos == 0 and holdings > 0:
                capital = holdings * act
                holdings = 0
                trades.append({"day": i, "action": "sell", "price": act, "value": capital})
            peak = max(peak, capital + holdings * act if holdings > 0 else capital)

        final_value = capital + holdings * (actuals[-1] if actuals else 0)
        total_return = ((final_value - 10000) / 10000) * 100

        return {
            "initial_capital": 10000,
            "final_value": round(final_value, 2),
            "total_return_pct": round(total_return, 2),
            "trade_count": len(trades),
            "peak_value": round(peak, 2),
            "max_drawdown_pct": round((peak - final_value) / peak * 100 if peak > 0 else 0, 2),
            "trades": trades,
        }


class BacktestingPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="backtesting",
            stages=[BacktestStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            bt = result.data.get("backtest", {})
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={
                    "return_pct": bt.get("total_return_pct", 0),
                    "max_drawdown": bt.get("max_drawdown_pct", 0),
                    "trades": bt.get("trade_count", 0),
                },
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
