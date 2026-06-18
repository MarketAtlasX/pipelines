from __future__ import annotations

import abc
import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pipelines.core.types import Event

logger = logging.getLogger(__name__)


class Trigger(abc.ABC):
    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    @abc.abstractmethod
    async def listen(self, handler: Callable[[Event], Any]) -> None: ...


class CronTrigger(Trigger):
    def __init__(self, name: str, cron: str) -> None:
        super().__init__(name)
        self.cron = cron

    async def listen(self, handler: Callable[[Event], Any]) -> None:
        logger.info("CronTrigger '%s' listening on schedule: %s", self.name, self.cron)
        while True:
            event = Event(source="cron", type="scheduled", data={"cron": self.cron})
            asyncio.create_task(self._safe_call(handler, event))
            await asyncio.sleep(self._to_seconds(self.cron))

    async def _safe_call(self, handler: Callable, event: Event) -> None:
        try:
            await handler(event)
        except Exception as e:
            logger.error("CronTrigger handler error: %s", e)

    def _to_seconds(self, cron: str) -> int:
        parts = cron.strip().split()
        return int(parts[0]) if len(parts) == 1 else 3600


class EventTrigger(Trigger):
    def __init__(self, name: str, event_type: str) -> None:
        super().__init__(name)
        self.event_type = event_type
        self._queue: asyncio.Queue[Event] = asyncio.Queue()

    async def push(self, event: Event) -> None:
        if event.type == self.event_type:
            await self._queue.put(event)

    async def listen(self, handler: Callable[[Event], Any]) -> None:
        logger.info("EventTrigger '%s' listening for type: %s", self.name, self.event_type)
        while True:
            event = await self._queue.get()
            try:
                await handler(event)
            except Exception as e:
                logger.error("EventTrigger handler error: %s", e)


class WebhookTrigger(Trigger):
    def __init__(self, name: str, path: str = "/webhook") -> None:
        super().__init__(name)
        self.path = path
        self._handlers: List[Callable] = []

    async def handle(self, payload: Dict[str, Any]) -> Event:
        event = Event(source="webhook", type="webhook", data=payload)
        for handler in self._handlers:
            try:
                await handler(event)
            except Exception as e:
                logger.error("Webhook handler error: %s", e)
        return event

    async def listen(self, handler: Callable[[Event], Any]) -> None:
        self._handlers.append(handler)
        logger.info("WebhookTrigger '%s' registered handler", self.name)
