from __future__ import annotations

import logging
from typing import Any, Dict, List

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)

EDGE_TYPES = {
    "mentions": {"color": "#95a5a6", "relation": "MENTIONS"},
    "affects": {"color": "#e74c3c", "relation": "AFFECTS"},
    "located_in": {"color": "#3498db", "relation": "LOCATED_IN"},
    "involves": {"color": "#2ecc71", "relation": "INVOLVES"},
    "impacts": {"color": "#f39c12", "relation": "IMPACTS"},
    "similar_to": {"color": "#9b59b6", "relation": "SIMILAR_TO"},
}


class EdgeBuilderStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("graph_edge_builder")

    async def run(self, event: Event, context: Context) -> Event:
        events = (
            event.data.get("summarized_events")
            or event.data.get("sentiment_events")
            or [event.model_dump()]
        )
        nodes = event.data.get("kg_nodes", [])
        edges = []

        node_map = {n["id"]: n for n in nodes}
        event_nodes = [n for n in nodes if n["type"] == "event"]

        for en in event_nodes:
            props = en.get("properties", {})
            entities = events[0].get("entities", {}) if events else {}

            for country in entities.get("countries", []):
                cid = f"country:{country.lower().replace(' ', '_')}"
                if cid in node_map:
                    edges.append({
                        "source": en["id"],
                        "target": cid,
                        "type": "mentions",
                        "relation": "MENTIONS",
                        "weight": 0.8,
                        "metadata": EDGE_TYPES["mentions"],
                    })

            for org in entities.get("organizations", []):
                oid = f"org:{org.lower().replace(' ', '_')}"
                if oid in node_map:
                    edges.append({
                        "source": en["id"],
                        "target": oid,
                        "type": "mentions",
                        "relation": "MENTIONS",
                        "weight": 0.7,
                        "metadata": EDGE_TYPES["mentions"],
                    })

            if en.get("properties", {}).get("sentiment", 0) < -0.3:
                edges.append({
                    "source": en["id"],
                    "target": "market:global",
                    "type": "affects",
                    "relation": "AFFECTS",
                    "weight": abs(props.get("sentiment", 0)),
                    "metadata": EDGE_TYPES["affects"],
                })

        event.data["kg_edges"] = edges
        event.data["edge_count"] = len(edges)
        logger.info("Built %d knowledge graph edges", len(edges))
        return event


class EdgeBuilderPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="kg_edge_builder",
            stages=[EdgeBuilderStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"edges": result.data.get("edge_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
