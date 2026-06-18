"""PipelineFactory — the brain that wires all pipelines together.

Collect → Clean → Transform → Enrich → Store
Orchestrates every pipeline type with DAG-based workflow execution.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Type

from pipelines.config.settings import PipelineSettings
from pipelines.core.base import Pipeline
from pipelines.core.types import Context, Event, Outcome, PipelineType
from pipelines.orchestration.dag import DAG, DAGNode
from pipelines.orchestration.executor import PipelineExecutor
from pipelines.orchestration.monitor import Monitor
from pipelines.orchestration.scheduler import Scheduler, ScheduleSpec
from pipelines.orchestration.triggers import CronTrigger, EventTrigger, WebhookTrigger
from pipelines.orchestration.workflow import WorkflowBuilder

logger = logging.getLogger(__name__)


class PipelineFactory:
    """Central factory that instantiates, registers, and orchestrates all pipelines."""

    def __init__(self, settings: Optional[PipelineSettings] = None) -> None:
        self.settings = settings or PipelineSettings()
        self._registry: Dict[str, Pipeline] = {}
        self._dags: Dict[str, DAG] = {}
        self.executor = PipelineExecutor(max_workers=self.settings.max_parallel_stages)
        self.scheduler = Scheduler()
        self.monitor = Monitor()
        self._triggers: Dict[str, Any] = {}
        self._on_start_handlers: List[Callable] = []
        self._on_complete_handlers: List[Callable] = []

    # ── Registration ──────────────────────────────────────────────

    def register(self, name: str, pipeline: Pipeline) -> Pipeline:
        self._registry[name] = pipeline
        pipeline.on_complete(lambda o: self.monitor.record(o))
        logger.info("Registered pipeline: %s", name)
        return pipeline

    def get(self, name: str) -> Optional[Pipeline]:
        return self._registry.get(name)

    def list_pipelines(self) -> Dict[str, str]:
        return {name: p.__class__.__name__ for name, p in self._registry.items()}

    # ── DAG Workflows ─────────────────────────────────────────────

    def create_dag(self, name: str) -> DAG:
        dag = DAG(name)
        self._dags[name] = dag
        return dag

    def build_workflow(self, name: str, dag: DAG) -> Any:
        builder = WorkflowBuilder(name)
        for node_name in dag.nodes:
            pipeline = self._registry.get(node_name)
            if pipeline:
                builder.add_pipeline(node_name, pipeline)
        for src, tgt, _ in dag.edges:
            builder.add_edge(src, tgt)
        return builder.build()

    # ── Execution ─────────────────────────────────────────────────

    async def run(self, name: str, event: Optional[Event] = None, **params: Any) -> Outcome:
        pipeline = self._registry.get(name)
        if not pipeline:
            raise ValueError(f"Pipeline '{name}' not registered")
        ctx = Context(
            pipeline=name,
            pipeline_type=PipelineType.INGESTION,
            params=params,
        )
        ev = event or Event(source="factory", type=name)
        return await self.executor.run(pipeline, ev, ctx)

    async def run_all(self, event: Optional[Event] = None, **params: Any) -> Dict[str, Outcome]:
        results = {}
        for name in self._registry:
            try:
                results[name] = await self.run(name, event, **params)
            except Exception as e:
                logger.error("Pipeline '%s' failed: %s", name, e)
        return results

    # ── Scheduling & Triggers ─────────────────────────────────────

    def schedule(self, name: str, cron: str) -> None:
        spec = ScheduleSpec(
            name=name,
            cron=cron,
            func=self.run,
            kwargs={"name": name},
        )
        self.scheduler.register(spec)

    def add_trigger(self, trigger: Any) -> None:
        self._triggers[trigger.name] = trigger

    async def start_scheduler(self) -> None:
        await self.scheduler.start()

    async def stop_scheduler(self) -> None:
        await self.scheduler.stop()

    # ── Event Handling ────────────────────────────────────────────

    def on_start(self, handler: Callable) -> None:
        self._on_start_handlers.append(handler)

    def on_complete(self, handler: Callable) -> None:
        self._on_complete_handlers.append(handler)

    # ── Summary ───────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        return {
            "registered_pipelines": len(self._registry),
            "dags": len(self._dags),
            "triggers": len(self._triggers),
            "runs": self.monitor.summary(),
        }


def build_pipelines(settings: Optional[PipelineSettings] = None) -> PipelineFactory:
    """Build and register every pipeline. Returns the factory ready to run."""
    factory = PipelineFactory(settings)

    # ── Ingestion ─────────────────────────────────────────────
    from pipelines.ingestion.gdelt import GDELTPipeline
    from pipelines.ingestion.newsapi import NewsAPIPipeline
    from pipelines.ingestion.rss import RSSPipeline
    from pipelines.ingestion.webhooks import WebhookPipeline

    factory.register("ingestion_gdelt", GDELTPipeline())
    factory.register("ingestion_newsapi", NewsAPIPipeline(api_key=""))
    factory.register("ingestion_rss", RSSPipeline([]))
    factory.register("ingestion_webhook", WebhookPipeline())

    # ── NLP Enrichment ────────────────────────────────────────
    from pipelines.nlp.embedding import EmbeddingPipeline
    from pipelines.nlp.entities import EntityExtractionPipeline
    from pipelines.nlp.sentiment import SentimentPipeline
    from pipelines.nlp.summarization import SummarizationPipeline

    factory.register("nlp_embedding", EmbeddingPipeline())
    factory.register("nlp_entities", EntityExtractionPipeline())
    factory.register("nlp_sentiment", SentimentPipeline())
    factory.register("nlp_summarization", SummarizationPipeline())

    # ── Knowledge Graph ───────────────────────────────────────
    from pipelines.kg.graph import KnowledgeGraphPipeline
    from pipelines.kg.nodes import NodeBuilderPipeline
    from pipelines.kg.edges import EdgeBuilderPipeline
    from pipelines.kg.queries import GraphQueryPipeline

    factory.register("kg_build", KnowledgeGraphPipeline())
    factory.register("kg_nodes", NodeBuilderPipeline())
    factory.register("kg_edges", EdgeBuilderPipeline())
    factory.register("kg_query", GraphQueryPipeline())

    # ── Features & Signals ────────────────────────────────────
    from pipelines.features.engineering import FeatureEngineeringPipeline
    from pipelines.features.signals import SignalGenerationPipeline
    from pipelines.features.store import FeatureStorePipeline

    factory.register("feature_engineering", FeatureEngineeringPipeline())
    factory.register("signal_generation", SignalGenerationPipeline())
    factory.register("feature_store", FeatureStorePipeline())

    # ── Training & Evaluation ─────────────────────────────────
    from pipelines.training.trainer import TrainingPipeline
    from pipelines.training.models import ModelRegistryPipeline
    from pipelines.training.evaluation import EvaluationPipeline

    factory.register("training", TrainingPipeline())
    factory.register("model_registry", ModelRegistryPipeline())
    factory.register("evaluation", EvaluationPipeline())

    # ── Forecasting ───────────────────────────────────────────
    from pipelines.forecasting.forecaster import ForecastingPipeline
    from pipelines.forecasting.models import ForecastModelsPipeline
    from pipelines.forecasting.ensemble import EnsemblePipeline

    factory.register("forecasting", ForecastingPipeline())
    factory.register("forecast_models", ForecastModelsPipeline())
    factory.register("forecast_ensemble", EnsemblePipeline())

    # ── Backtesting ───────────────────────────────────────────
    from pipelines.backtesting.engine import BacktestingPipeline
    from pipelines.backtesting.metrics import MetricsPipeline
    from pipelines.backtesting.reporting import ReportingPipeline

    factory.register("backtesting", BacktestingPipeline())
    factory.register("backtest_metrics", MetricsPipeline())
    factory.register("backtest_reporting", ReportingPipeline())

    # ── Streaming ─────────────────────────────────────────────
    from pipelines.streaming.kafka import KafkaPipeline
    from pipelines.streaming.processor import StreamProcessorPipeline
    from pipelines.streaming.websocket import WebSocketPipeline
    from pipelines.streaming.sink import StreamSinkPipeline

    factory.register("streaming_kafka", KafkaPipeline())
    factory.register("streaming_processor", StreamProcessorPipeline())
    factory.register("streaming_websocket", WebSocketPipeline())
    factory.register("streaming_sink", StreamSinkPipeline())

    # ── Intelligence ──────────────────────────────────────────
    from pipelines.intelligence.conflict import ConflictIntelligencePipeline
    from pipelines.intelligence.economic import EconomicIntelligencePipeline
    from pipelines.intelligence.alternative.ais import AISVesselTrackingPipeline
    from pipelines.intelligence.alternative.flights import FlightTrackingPipeline
    from pipelines.intelligence.alternative.satellite import SatelliteImageryPipeline
    from pipelines.intelligence.alternative.commodities import CommodityFlowsPipeline
    from pipelines.intelligence.news.global_news import GlobalNewsPipeline

    factory.register("intel_conflict", ConflictIntelligencePipeline())
    factory.register("intel_economic", EconomicIntelligencePipeline())
    factory.register("intel_ais", AISVesselTrackingPipeline())
    factory.register("intel_flights", FlightTrackingPipeline())
    factory.register("intel_satellite", SatelliteImageryPipeline())
    factory.register("intel_commodities", CommodityFlowsPipeline())
    factory.register("intel_global_news", GlobalNewsPipeline())

    # ── Event Similarity (the specific one) ───────────────────
    from pipelines.similarity.pipeline import EventSimilarityPipeline
    factory.register("event_similarity", EventSimilarityPipeline())

    # ── Explainability ────────────────────────────────────────
    from pipelines.explainability.shap import SHAPPipeline
    from pipelines.explainability.graph_paths import GraphPathPipeline
    from pipelines.explainability.analogs import HistoricalAnalogsPipeline
    from pipelines.explainability.generator import ExplanationGeneratorPipeline

    factory.register("explainability_shap", SHAPPipeline())
    factory.register("explainability_graph_paths", GraphPathPipeline())
    factory.register("explainability_analogs", HistoricalAnalogsPipeline())
    factory.register("explainability_generator", ExplanationGeneratorPipeline())

    # ── Daily Pipeline ────────────────────────────────────────
    from pipelines.daily.pipeline import DailyPipeline
    factory.register("daily", DailyPipeline())

    # ── Real-Time Pipeline ────────────────────────────────────
    from pipelines.realtime.pipeline import RealTimePipeline
    factory.register("realtime", RealTimePipeline())

    # ── Build the master DAG ──────────────────────────────────
    master_dag = factory.create_dag("master")

    # Ingestion → NLP → KG → Features → Training → Forecasting
    for node in [
        "ingestion_gdelt", "ingestion_newsapi", "ingestion_rss",
        "nlp_embedding", "nlp_entities", "nlp_sentiment", "nlp_summarization",
        "kg_build", "feature_engineering", "signal_generation",
        "training", "forecasting", "backtesting",
        "event_similarity", "explainability_shap",
        "intel_global_news", "intel_conflict", "intel_economic",
        "daily", "realtime",
    ]:
        master_dag.add_node(DAGNode(node))

    # Wire the DAG: ingestion → nlp → kg → features → signals → forecasting/backtesting
    ingestion_nodes = ["ingestion_gdelt", "ingestion_newsapi", "ingestion_rss"]
    nlp_nodes = ["nlp_embedding", "nlp_entities", "nlp_sentiment", "nlp_summarization"]

    for ing in ingestion_nodes:
        for nlp in nlp_nodes:
            master_dag.add_edge(ing, nlp)

    for nlp in nlp_nodes:
        master_dag.add_edge(nlp, "kg_build")

    master_dag.add_edge("kg_build", "feature_engineering")
    master_dag.add_edge("feature_engineering", "signal_generation")
    master_dag.add_edge("signal_generation", "training")
    master_dag.add_edge("training", "forecasting")
    master_dag.add_edge("forecasting", "backtesting")
    master_dag.add_edge("feature_engineering", "event_similarity")
    master_dag.add_edge("training", "explainability_shap")

    for ing in ingestion_nodes:
        master_dag.add_edge(ing, "intel_global_news")
    master_dag.add_edge("intel_global_news", "intel_conflict")
    master_dag.add_edge("intel_global_news", "intel_economic")

    # Daily and realtime as top-level workflows
    master_dag.add_edge("ingestion_gdelt", "daily")
    master_dag.add_edge("ingestion_newsapi", "realtime")
    master_dag.add_edge("ingestion_rss", "realtime")

    logger.info(
        "MarketAtlas Pipelines Factory initialized with %d pipelines across %d DAG nodes",
        len(factory._registry),
        len(master_dag.nodes),
    )

    # ── Schedules ─────────────────────────────────────────────
    factory.schedule("daily", "86400")      # Every 24 hours
    factory.schedule("intel_global_news", "3600")  # Every hour
    factory.schedule("intel_ais", "1800")   # Every 30 min
    factory.schedule("intel_flights", "1800")

    return factory
