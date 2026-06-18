from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Set

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class WebSocketServerStage(PipelineStage):
    def __init__(self, host: str = "0.0.0.0", port: int = 8765) -> None:
        super().__init__("websocket_server")
        self.host = host
        self.port = port
        self._connections: Set[Any] = set()

    async def run(self, event: Event, context: Context) -> Event:
        payload = {
            "type": "pipeline_update",
            "pipeline": context.pipeline,
            "event_id": event.id,
            "data": event.data,
            "timestamp": event.timestamp.isoformat(),
        }
        await self.broadcast(payload)
        event.metadata["websocket_broadcast"] = True
        logger.debug("Broadcast to %d WS clients", len(self._connections))
        return event

    async def broadcast(self, payload: Dict[str, Any]) -> None:
        message = json.dumps(payload, default=str)
        for ws in self._connections.copy():
            try:
                await ws.send_text(message)
            except Exception:
                self._connections.discard(ws)

    def add_connection(self, ws: Any) -> None:
        self._connections.add(ws)

    def remove_connection(self, ws: Any) -> None:
        self._connections.discard(ws)


class WebSocketPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="streaming_websocket",
            stages=[WebSocketServerStage()],
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
