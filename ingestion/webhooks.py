from __future__ import annotations

import logging
from typing import Any, Dict

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.data_factory.collector import WebhookCollector
from pipelines.data_factory.transformer import EventBuilder, PipelineTransformer

logger = logging.getLogger(__name__)


class WebhookReceiveStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("webhook_receive")
        self.collector = WebhookCollector()

    async def receive(self, payload: Dict[str, Any]) -> Event:
        return await self.collector.receive(payload)

    async def run(self, event: Event, context: Context) -> Event:
        logger.info("Webhook received: %s", event.id)
        return event


class WebhookTransformStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("webhook_transform")
        self.transformer = PipelineTransformer([EventBuilder()])

    async def run(self, event: Event, context: Context) -> Event:
        event = await self.transformer.transform(event)
        context.state["webhook_count"] = context.state.get("webhook_count", 0) + 1
        return event


class WebhookPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="webhook_ingestion",
            stages=[
                WebhookReceiveStage(),
                WebhookTransformStage(),
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
