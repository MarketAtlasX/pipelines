from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Dict, List, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class KafkaConsumeStage(PipelineStage):
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "raw-events",
        group_id: str = "pipelines",
    ) -> None:
        super().__init__("kafka_consume")
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._running = False
        self._handlers: List[Callable] = []

    async def run(self, event: Event, context: Context) -> Event:
        self._running = True
        logger.info("Kafka consumer started on topic '%s'", self.topic)
        event.data["kafka_consumer"] = True
        return event

    async def consume(self) -> None:
        self._running = True
        while self._running:
            await asyncio.sleep(0.1)

    async def stop(self) -> None:
        self._running = False


class KafkaProduceStage(PipelineStage):
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "enriched-events",
    ) -> None:
        super().__init__("kafka_produce")
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    async def run(self, event: Event, context: Context) -> Event:
        logger.debug("Kafka produced event %s to '%s'", event.id[:8], self.topic)
        event.metadata["kafka_produced"] = True
        event.metadata["kafka_topic"] = self.topic
        return event


class KafkaPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="streaming_kafka",
            stages=[
                KafkaConsumeStage(),
                KafkaProduceStage(),
            ],
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
