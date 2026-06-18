from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import networkx as nx

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class GraphQueryStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("graph_query")

    async def run(self, event: Event, context: Context) -> Event:
        G: nx.DiGraph = event.data.get("graph", nx.DiGraph())
        query = context.params.get("query", {})
        results = {}

        if query.get("type") == "shortest_path":
            source = query.get("source")
            target = query.get("target")
            if source and target and source in G and target in G:
                try:
                    path = nx.shortest_path(G, source=source, target=target)
                    results["path"] = path
                    results["length"] = len(path) - 1
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    results["path"] = None

        elif query.get("type") == "neighbors":
            node = query.get("node")
            if node and node in G:
                neighbors = list(G.neighbors(node))
                results["neighbors"] = neighbors
                results["count"] = len(neighbors)

        elif query.get("type") == "subgraph":
            nodes_subset = query.get("nodes", [])
            valid = [n for n in nodes_subset if n in G]
            sg = G.subgraph(valid)
            results["subgraph_nodes"] = sg.number_of_nodes()
            results["subgraph_edges"] = sg.number_of_edges()

        elif query.get("type") == "impact_path":
            event_node = query.get("event")
            if event_node and event_node in G:
                paths = []
                for node in G.nodes():
                    if node != event_node and G.has_edge(event_node, node):
                        paths.append({"from": event_node, "to": node, "weight": G[event_node][node].get("weight", 1.0)})
                results["impacts"] = sorted(paths, key=lambda x: x["weight"], reverse=True)[:20]

        event.data["query_results"] = results
        logger.info("Graph query '%s' returned %d results", query.get("type", "unknown"), len(results))
        return event


class GraphQueryPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="kg_query",
            stages=[GraphQueryStage()],
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
