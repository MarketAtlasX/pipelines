from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ScheduleSpec:
    name: str
    cron: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None


class Scheduler:
    def __init__(self) -> None:
        self._schedules: Dict[str, ScheduleSpec] = {}
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def register(self, spec: ScheduleSpec) -> None:
        self._schedules[spec.name] = spec
        logger.info("Registered schedule: %s [%s]", spec.name, spec.cron)

    def remove(self, name: str) -> None:
        self._schedules.pop(name, None)

    async def start(self) -> None:
        self._running = True
        logger.info("Scheduler started with %d schedules", len(self._schedules))
        while self._running:
            now = datetime.utcnow()
            for spec in self._schedules.values():
                if not spec.enabled:
                    continue
                if spec.next_run is None or now >= spec.next_run:
                    spec.last_run = now
                    spec.next_run = now + timedelta(seconds=self._parse_interval(spec.cron))
                    asyncio.create_task(self._execute(spec))
            await asyncio.sleep(1)

    async def stop(self) -> None:
        self._running = False
        logger.info("Scheduler stopped")

    async def _execute(self, spec: ScheduleSpec) -> None:
        try:
            logger.info("Executing scheduled task: %s", spec.name)
            if asyncio.iscoroutinefunction(spec.func):
                await spec.func(*spec.args, **spec.kwargs)
            else:
                spec.func(*spec.args, **spec.kwargs)
        except Exception as e:
            logger.error("Scheduled task '%s' failed: %s", spec.name, e)

    def _parse_interval(self, cron: str) -> int:
        parts = cron.strip().split()
        if len(parts) == 1:
            return int(parts[0])
        return 3600
