from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx

logger = logging.getLogger(__name__)


class DAGNode:
    def __init__(self, name: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        self.name = name
        self.metadata = metadata or {}

    def __repr__(self) -> str:
        return f"DAGNode({self.name})"

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DAGNode):
            return NotImplemented
        return self.name == other.name


class DAGEdge:
    def __init__(self, source: str, target: str, condition: Optional[str] = None) -> None:
        self.source = source
        self.target = target
        self.condition = condition

    def __repr__(self) -> str:
        return f"DAGEdge({self.source} -> {self.target})"


class DAG:
    def __init__(self, name: str) -> None:
        self.name = name
        self._graph = nx.DiGraph()

    def add_node(self, node: DAGNode) -> DAGNode:
        self._graph.add_node(node.name, metadata=node.metadata)
        return node

    def add_edge(self, source: str, target: str, condition: Optional[str] = None) -> None:
        if source not in self._graph:
            raise ValueError(f"Node '{source}' not found in DAG '{self.name}'")
        if target not in self._graph:
            raise ValueError(f"Node '{target}' not found in DAG '{self.name}'")
        self._graph.add_edge(source, target, condition=condition)

    def add_pipeline(
        self, source: str, target: str, condition: Optional[str] = None
    ) -> None:
        self.add_edge(source, target, condition)

    @property
    def nodes(self) -> List[str]:
        return list(self._graph.nodes())

    @property
    def edges(self) -> List[Tuple[str, str, Optional[str]]]:
        return [
            (u, v, self._graph.edges[u, v].get("condition"))
            for u, v in self._graph.edges()
        ]

    def upstream(self, node: str) -> List[str]:
        return list(self._graph.predecessors(node))

    def downstream(self, node: str) -> List[str]:
        return list(self._graph.successors(node))

    def topological_sort(self) -> List[str]:
        try:
            return list(nx.topological_sort(self._graph))
        except nx.NetworkXUnfeasible:
            cycles = list(nx.simple_cycles(self._graph))
            logger.error("Cycle detected in DAG '%s': %s", self.name, cycles)
            raise ValueError(f"DAG '{self.name}' contains cycles: {cycles}")

    def execution_layers(self) -> List[List[str]]:
        levels: Dict[str, int] = {}
        for node in self.topological_sort():
            preds = self.upstream(node)
            levels[node] = 0 if not preds else max(levels[p] for p in preds) + 1
        layers: Dict[int, List[str]] = {}
        for node, level in levels.items():
            layers.setdefault(level, []).append(node)
        return [layers[i] for i in sorted(layers)]

    def roots(self) -> List[str]:
        return [n for n in self._graph.nodes() if self._graph.in_degree(n) == 0]

    def leaves(self) -> List[str]:
        return [n for n in self._graph.nodes() if self._graph.out_degree(n) == 0]

    def validate(self) -> bool:
        try:
            self.topological_sort()
            return True
        except ValueError:
            return False

    def __repr__(self) -> str:
        return f"DAG({self.name}, nodes={len(self._graph)}, edges={self._graph.number_of_edges()})"
