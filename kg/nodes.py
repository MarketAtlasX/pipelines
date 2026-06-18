from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

NODE_TYPES = {
    "event": {"color": "#e74c3c", "icon": "bolt"},
    "country": {"color": "#3498db", "icon": "flag"},
    "organization": {"color": "#2ecc71", "icon": "building"},
    "person": {"color": "#f39c12", "icon": "user"},
    "market": {"color": "#9b59b6", "icon": "chart-line"},
    "commodity": {"color": "#1abc9c", "icon": "oil-can"},
    "topic": {"color": "#e67e22", "icon": "tag"},
}


class NodeBuilderStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("graph_node_builder")

    async def run(self, event: Event, context: Context) -> Event:
        events = (
            event.data.get("summarized_events")
            or event.data.get("sentiment_events")
            or [event.model_dump()]
        )
        nodes = []
        for e in events:
            if not isinstance(e, dict):
                continue
            node = {
                "id": e.get("id", uuid.uuid4().hex),
                "type": "event",
                "label": e.get("title", "Untitled Event")[:80],
                "properties": {
                    "source": e.get("source", "unknown"),
                    "published": e.get("published", ""),
                    "sentiment": e.get("sentiment", {}).get("score", 0),
                    "url": e.get("url", ""),
                    "summary": e.get("summary", ""),
                },
                "metadata": NODE_TYPES["event"],
            }
            nodes.append(node)

            entities = e.get("entities", {})
            for country in entities.get("countries", []):
                nodes.append({
                    "id": f"country:{country.lower().replace(' ', '_')}",
                    "type": "country",
                    "label": country,
                    "properties": {},
                    "metadata": NODE_TYPES["country"],
                })
            for org in entities.get("organizations", []):
                nodes.append({
                    "id": f"org:{org.lower().replace(' ', '_')}",
                    "type": "organization",
                    "label": org,
                    "properties": {},
                    "metadata": NODE_TYPES["organization"],
                })

        event.data["kg_nodes"] = nodes
        event.data["node_count"] = len(nodes)
        logger.info("Built %d knowledge graph nodes", len(nodes))
        return event


class NodeBuilderPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="kg_node_builder",
            stages=[NodeBuilderStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"nodes": result.data.get("node_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
