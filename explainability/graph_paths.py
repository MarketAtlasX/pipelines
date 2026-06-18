from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import networkx as nx

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class GraphPathExtractionStage(PipelineStage):
    def __init__(self, max_paths: int = 5) -> None:
        super().__init__("graph_path_extraction")
        self.max_paths = max_paths

    async def run(self, event: Event, context: Context) -> Event:
        G: nx.DiGraph = event.data.get("graph", nx.DiGraph())
        target = context.params.get("target_node") or (
            list(G.nodes())[-1] if G.nodes() else None
        )
        sources = context.params.get("source_nodes") or (
            [n for n in G.nodes() if G.in_degree(n) == 0]
        )

        paths = []
        for src in sources[:3]:
            if src != target and src in G and target in G:
                try:
                    for path in nx.all_simple_paths(G, src, target, cutoff=5):
                        if len(paths) >= self.max_paths:
                            break
                        edge_info = []
                        for i in range(len(path) - 1):
                            edge_data = G.get_edge_data(path[i], path[i + 1])
                            edge_info.append({
                                "source": path[i],
                                "target": path[i + 1],
                                "relation": edge_data.get("relation", "related") if edge_data else "related",
                                "weight": edge_data.get("weight", 1.0) if edge_data else 1.0,
                            })
                        paths.append({
                            "path": path,
                            "edges": edge_info,
                            "length": len(path) - 1,
                        })
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

        event.data["graph_paths"] = paths
        event.data["path_count"] = len(paths)
        logger.info("Extracted %d graph paths", len(paths))
        return event


class GraphPathPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="explainability_graph_paths",
            stages=[GraphPathExtractionStage()],
        )

    async def run(self, event: Event, context: Context) -> Outcome:
        self.state.start()
        try:
            result = await self.execute(event, context)
            outcome = Outcome(
                context=context,
                status=PipelineStatus.SUCCESS,
                events=[result],
                metrics={"paths": result.data.get("path_count", 0)},
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            outcome = Outcome(context=context, status=PipelineStatus.FAILED, error=str(e))
            self.state.fail(outcome)
            return outcome
