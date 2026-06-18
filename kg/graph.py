from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import networkx as nx

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class GraphBuildStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("graph_build")

    async def run(self, event: Event, context: Context) -> Event:
        G = nx.DiGraph()
        nodes = event.data.get("kg_nodes", [])
        edges = event.data.get("kg_edges", [])

        for n in nodes:
            G.add_node(n["id"], **n.get("properties", {}), type=n["type"], label=n["label"])

        for e in edges:
            G.add_edge(
                e["source"],
                e["target"],
                relation=e.get("relation", "MENTIONS"),
                weight=e.get("weight", 1.0),
                type=e.get("type", "mentions"),
            )

        event.data["graph"] = G
        event.data["graph_stats"] = {
            "nodes": G.number_of_nodes(),
            "edges": G.number_of_edges(),
            "density": round(nx.density(G), 4),
        }
        logger.info("Graph built: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
        return event


class GraphAnalyzeStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("graph_analysis")

    async def run(self, event: Event, context: Context) -> Event:
        G: nx.DiGraph = event.data.get("graph")
        if G is None or G.number_of_nodes() == 0:
            logger.warning("No graph to analyze")
            return event

        try:
            degrees = dict(G.degree())
            centralities = nx.degree_centrality(G)
            top_nodes = sorted(centralities.items(), key=lambda x: x[1], reverse=True)[:10]

            event.data["graph_analysis"] = {
                "top_nodes": [{"id": n, "centrality": round(c, 4)} for n, c in top_nodes],
                "avg_degree": round(sum(degrees.values()) / max(len(degrees), 1), 2),
            }
        except Exception as e:
            logger.warning("Graph analysis error: %s", e)

        return event


class KnowledgeGraphPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="knowledge_graph",
            stages=[
                GraphBuildStage(),
                GraphAnalyzeStage(),
            ],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            stats = result.data.get("graph_stats", {})
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={
                    "nodes": stats.get("nodes", 0),
                    "edges": stats.get("edges", 0),
                    "density": stats.get("density", 0),
                },
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
