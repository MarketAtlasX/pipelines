from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from pipelines.core.base import Pipeline
from pipelines.core.types import Context, Event, Outcome, PipelineStatus
from pipelines.orchestration.dag import DAG, DAGNode

logger = logging.getLogger(__name__)


class WorkflowBuilder:
    def __init__(self, name: str) -> None:
        self.name = name
        self._dag = DAG(name)
        self._pipelines: Dict[str, Pipeline] = {}

    def add_pipeline(self, name: str, pipeline: Pipeline) -> DAGNode:
        node = DAGNode(name, {"pipeline": pipeline})
        self._dag.add_node(node)
        self._pipelines[name] = pipeline
        return node

    def add_edge(self, source: str, target: str) -> None:
        self._dag.add_edge(source, target)

    def build(self) -> Workflow:
        if not self._dag.validate():
            raise ValueError(f"Workflow '{self.name}' DAG validation failed")
        return Workflow(self.name, self._dag, self._pipelines)


class Workflow:
    def __init__(self, name: str, dag: DAG, pipelines: Dict[str, Pipeline]) -> None:
        self.name = name
        self._dag = dag
        self._pipelines = pipelines
        self._results: Dict[str, Outcome] = {}
        self._handlers: Dict[str, List[Callable]] = {
            "success": [],
            "failure": [],
            "complete": [],
        }

    async def run(
        self,
        initial_event: Event,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Outcome]:
        context = Context(
            pipeline=self.name,
            pipeline_type=self.name,  # type: ignore
            params=params or {},
        )
        layers = self._dag.execution_layers()

        logger.info(
            "Workflow '%s' starting, %d layers, %d pipelines",
            self.name,
            len(layers),
            len(self._pipelines),
        )

        node_results: Dict[str, Event] = {}

        for layer_idx, layer in enumerate(layers):
            tasks = {}
            for node_name in layer:
                pipeline = self._pipelines.get(node_name)
                if not pipeline:
                    logger.warning("No pipeline registered for node '%s'", node_name)
                    continue

                upstream = self._dag.upstream(node_name)
                if not upstream:
                    event = initial_event
                else:
                    last_upstream = upstream[-1]
                    event = node_results.get(last_upstream, initial_event)

                tasks[node_name] = pipeline.run(event, context)

            if tasks:
                results = await asyncio.gather(
                    *tasks.values(), return_exceptions=True
                )
                for node_name, result in zip(tasks.keys(), results):
                    if isinstance(result, Exception):
                        logger.error(
                            "Pipeline '%s' failed: %s", node_name, result
                        )
                        outcome = Outcome(
                            context=context,
                            status=PipelineStatus.FAILED,
                            error=str(result),
                        )
                        self._results[node_name] = outcome
                        self._emit("failure", outcome)
                    else:
                        self._results[node_name] = result
                        node_results[node_name] = (
                            result.events[-1] if result.events else initial_event
                        )
                        self._emit("success", result)

        self._emit("complete", None)
        return self._results

    def on(self, event: str, handler: Callable) -> Workflow:
        self._handlers.setdefault(event, []).append(handler)
        return self

    def _emit(self, event: str, data: Any) -> None:
        for handler in self._handlers.get(event, []):
            try:
                handler(data)
            except Exception as e:
                logger.exception("Handler error on event '%s': %s", event, e)
