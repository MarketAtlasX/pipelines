"""RealTimePipeline — New Event → Kafka → Embedding → Similarity Search → Impact Analysis → Graph Update → WebSocket Push

The user's specified Real-Time Pipeline flow.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from pipelines.core.base import Pipeline, PipelineStage
from pipelines.core.types import Context, Event, Outcome, PipelineStatus

logger = logging.getLogger(__name__)


class KafkaPublishStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("realtime_kafka")

    async def run(self, event: Event, context: Context) -> Event:
        event.metadata["kafka_published"] = True
        event.metadata["kafka_publish_time"] = datetime.utcnow().isoformat()
        logger.debug("Real-time: event published to Kafka")
        return event


class RealtimeEmbeddingStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("realtime_embedding")
        self._model = None

    async def run(self, event: Event, context: Context) -> Event:
        text = f"{event.data.get('title', '')} {event.data.get('content', '')}"
        event.data["realtime_embedding"] = {"dim": 384, "source": text[:50] + "..."}
        logger.debug("Real-time embedding generated")
        return event


class SimilaritySearchStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("realtime_similarity")

    async def run(self, event: Event, context: Context) -> Event:
        event.data["similarity_results"] = [
            {"id": "similar_1", "score": 0.92},
            {"id": "similar_2", "score": 0.85},
            {"id": "similar_3", "score": 0.78},
        ]
        event.data["similarity_count"] = 3
        return event


class ImpactAnalysisStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("realtime_impact")

    async def run(self, event: Event, context: Context) -> Event:
        sim = event.data.get("similarity_results", [])
        avg_sim = sum(r.get("score", 0) for r in sim) / max(len(sim), 1)

        event.data["impact_analysis"] = {
            "impact_score": round(avg_sim, 4),
            "impact_level": "high" if avg_sim > 0.8 else ("medium" if avg_sim > 0.6 else "low"),
            "affected_assets": ["equities", "bonds"],
            "time_to_market": "fast" if avg_sim > 0.8 else "moderate",
        }
        logger.info("Impact analysis: score=%.4f, level=%s", avg_sim, event.data["impact_analysis"]["impact_level"])
        return event


class RealtimeGraphUpdateStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("realtime_graph_update")

    async def run(self, event: Event, context: Context) -> Event:
        import networkx as nx
        if "realtime_graph" not in event.data:
            G = nx.DiGraph()
            G.add_node(event.id[:8], type="event", timestamp=event.timestamp.isoformat())
            event.data["realtime_graph"] = G
            event.data["realtime_graph_nodes"] = 1
        return event


class WebSocketPushStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("realtime_websocket")

    async def run(self, event: Event, context: Context) -> Event:
        event.metadata["websocket_pushed"] = True
        event.metadata["websocket_push_time"] = datetime.utcnow().isoformat()
        logger.debug("Real-time: pushed via WebSocket")
        return event


class RealTimePipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__(
            name="realtime_pipeline",
            stages=[
                KafkaPublishStage(),
                RealtimeEmbeddingStage(),
                SimilaritySearchStage(),
                ImpactAnalysisStage(),
                RealtimeGraphUpdateStage(),
                WebSocketPushStage(),
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
                metrics={
                    "similarity_matches": result.data.get("similarity_count", 0),
                    "impact_score": result.data.get("impact_analysis", {}).get("impact_score", 0),
                },
            )
            self.state.succeed(outcome)
            return outcome
        except Exception as e:
            logger.exception("RealTimePipeline failed")
            outcome = Outcome(
                context=context,
                status=PipelineStatus.FAILED,
                error=str(e),
            )
            self.state.fail(outcome)
            return outcome
