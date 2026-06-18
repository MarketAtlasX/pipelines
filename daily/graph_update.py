from __future__ import annotations

import logging

import networkx as nx

from pipelines.core.base import PipelineStage
from pipelines.core.types import Context, Event

logger = logging.getLogger(__name__)


class DailyGraphUpdateStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("daily_graph_update")

    async def run(self, event: Event, context: Context) -> Event:
        events_data = event.data.get("enriched_events", [])
        G = nx.DiGraph()

        for e in events_data:
            if not isinstance(e, dict):
                continue
            node_id = e.get("id", f"event_{hash(str(e)) % 100000}")
            G.add_node(
                node_id,
                title=e.get("title", ""),
                sentiment=e.get("sentiment", {}).get("score", 0) if isinstance(e.get("sentiment"), dict) else 0,
                entities=e.get("entities", {}),
                timestamp=e.get("published", ""),
            )

            entities = e.get("entities", {})
            if isinstance(entities, dict):
                for country in entities.get("countries", []):
                    cid = f"country:{country.lower().replace(' ', '_')}"
                    G.add_edge(node_id, cid, relation="MENTIONS", weight=0.8)

                for org in entities.get("organizations", []):
                    oid = f"org:{org.lower().replace(' ', '_')}"
                    G.add_edge(node_id, oid, relation="MENTIONS", weight=0.6)

        event.data["daily_graph"] = G
        event.data["daily_graph_nodes"] = G.number_of_nodes()
        event.data["daily_graph_edges"] = G.number_of_edges()
        logger.info("Graph updated: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
        return event
