# MarketAtlas Pipelines

> *The data factory of MarketAtlas — 42 pipelines across 14 domains, orchestrated by a DAG brain.*

A modular, event-driven **data pipeline factory** for geopolitical risk intelligence, market impact analysis, and alternative data processing. This is the engine that collects, cleans, transforms, enriches, stores, analyzes, and forecasts event data at scale.

---

## The "Brain": PipelineFactory

```python
PipelineFactory  # The master orchestrator
  ├── Registers 42 pipelines across 14 domains
  ├── Builds a DAG for dependency-aware execution
  ├── Wires schedules (cron, event, webhook triggers)
  ├── Runs pipelines in topological order with asyncio.gather()
  └── Tracks every run with metrics and outcomes
```

---

## End-to-End Data Flow

```
                     COLLECT                           CLEAN                         TRANSFORM
   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐
   │  GDELT ──┐                                                                                  │
   │  NewsAPI ─┤  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
   │  RSS ────┼─►│ Collect  │─►│  Clean   │─►│Transform │─►│ Enrich   │─►│  Store   │          │
   │  Webhook─┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘          │
   │                                                                                             │
   │                     ANALYZE                           FORECAST                     STREAM   │
   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
   │  │Pipeline  │─►│   NLP    │─►│   KG     │─►│ Features │─►│ Training │─►│Forecast  │       │
   │  │Factory   │  │Embeddings│  │ Nodes    │  │ Signals  │  │ Random   │  │ Prophet  │       │
   │  │(DAG)     │  │Entities  │  │ Edges    │  │ Store    │  │ Forest   │  │ LSTM     │       │
   │  │          │  │Sentiment │  │ Graph    │  │          │  │ Evaluate │  │ Ensemble │       │
   │  │          │  │Summary   │  │ Queries  │  │          │  │ Registry │  │          │       │
   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │
   │                                                                                             │
   │              BACKTEST                     EXPLAIN                     REAL-TIME             │
   │         ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
   │         │Simulate  │─►│ Metrics  │─►│   SHAP   │─►│   Graph  │─►│  Kafka   │              │
   │         │  Trades  │  │ Sharpe   │  │  Paths   │  │  Analogs │  │ WebSocket│              │
   │         │  P&L     │  │Drawdown  │  │Generator │  │  History │  │  Stream  │              │
   │         └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘              │
   └─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 42 Pipelines Across 14 Domains

| Domain | Pipelines | What They Do |
|--------|-----------|-------------|
| **Ingestion** | GDELT, NewsAPI, RSS, Webhooks | Collect raw data from 4 sources |
| **NLP** | Embedding, Entities, Sentiment, Summarization | Extract meaning from text |
| **Knowledge Graph** | Nodes, Edges, Graph, Queries | Build & query relationship networks |
| **Features** | Engineering, Signals, Store | Extract ML features & generate signals |
| **Training** | Trainer, Model Registry, Evaluation | Train & track ML models |
| **Forecasting** | Prophet, LSTM, Ensemble | Predict sentiment & risk trends |
| **Backtesting** | Engine, Metrics, Reporting | Simulate & evaluate trading strategies |
| **Streaming** | Kafka, Processor, WebSocket, Sink | Real-time event streaming |
| **Similarity** | Embed, Search, Match, Links | Find analogous events |
| **Explainability** | SHAP, Graph Paths, Analogs, Generator | Understand why decisions were made |
| **Daily** | 7-stage end-to-end pipeline | Batch processing every 24h |
| **Real-time** | 6-stage streaming pipeline | Live event processing |
| **Intelligence** | Conflict, Economic | Geopolitical & economic analysis |
| **Alternative Data** | AIS, Flights, Satellite, Commodities | Non-traditional data sources |

---

## Pipeline Catalog (All 42)

### Ingestion Layer
| Pipeline | Stages | Schedule |
|----------|--------|----------|
| `GDELTPipeline` | Fetch → Clean | Every 15m |
| `NewsAPIPipeline` | Fetch → Clean | Every 30m |
| `RSSPipeline` | Fetch → Transform | Every 1h |
| `WebhookPipeline` | Receive → Transform | On-demand |

### NLP Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `EmbeddingPipeline` | Vector generation | Sentence embeddings via all-MiniLM-L6-v2 |
| `EntityExtractionPipeline` | Country/org extraction | Regex + keyword matching |
| `SentimentPipeline` | Lexicon scoring | Positive/negative word counting |
| `SummarizationPipeline` | Extractive summaries | Keyword relevance scoring |

### Knowledge Graph Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `NodeBuilderPipeline` | Event/Country/Org nodes | Typed entity creation |
| `EdgeBuilderPipeline` | MENTIONS/AFFECTS edges | Co-occurrence + sentiment inference |
| `KnowledgeGraphPipeline` | NetworkX DiGraph | Centrality analysis |
| `GraphQueryPipeline` | Shortest path, neighbors, subgraph | Relationship traversal |

### Features & Signals Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `FeatureEngineeringPipeline` | Vector extraction, statistics | Numeric ML features |
| `SignalGenerationPipeline` | Market risk/opportunity signals | Threshold-based |
| `FeatureStorePipeline` | Redis persistence | Feature caching |

### Training Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `TrainingPipeline` | RandomForest (100 estimators) | Sentiment-based classification |
| `ModelRegistryPipeline` | Register/list/get | In-memory model registry |
| `EvaluationPipeline` | Classification report, confusion matrix | sklearn metrics |

### Forecasting Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `ForecastingPipeline` | 30-day trend projection | Noise + drift + confidence bounds |
| `ForecastModelsPipeline` | Prophet + LSTM + Ensemble | Weighted combination |

### Backtesting Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `BacktestingPipeline` | Trade simulation | P&L, drawdown, trade count |
| `MetricsPipeline` | Sharpe, win rate, avg return | Performance metrics |
| `ReportingPipeline` | Structured reports | Combined results |

### Streaming Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `KafkaPipeline` | Consume → Produce | Topic-to-topic streaming |
| `StreamProcessorPipeline` | Transform → Filter | Signal processing |
| `WebSocketPipeline` | Broadcast updates | Real-time client push |
| `StreamSinkPipeline` | Redis persistence | Stream storage |

### Event Similarity Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `EventSimilarityPipeline` | Embed → Qdrant → Match → Outcomes → Store | Find past analogs |

### Explainability Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `SHAPPipeline` | TreeExplainer analysis | Feature importance |
| `GraphPathPipeline` | Causal path extraction | KG-based reasoning |
| `HistoricalAnalogsPipeline` | Sentiment/type matching | Historical precedent |
| `ExplanationGeneratorPipeline` | NL explanation assembly | Human-readable output |

### End-to-End Pipelines
| Pipeline | Stages | Cadence |
|----------|--------|---------|
| `DailyPipeline` | Dedup → Enrich → Graph → Features → Signals → Store | Every 24h |
| `RealTimePipeline` | Kafka → Embed → Similarity → Impact → Graph → WebSocket | Continuous |

### Intelligence Layer
| Pipeline | Stages | Purpose |
|----------|--------|---------|
| `ConflictIntelligencePipeline` | Escalation detection, active zones, trigger monitoring | Keyword analysis |
| `EconomicIntelligencePipeline` | GDP, inflation, trade, interest rate detection | Text analysis |
| `AISVesselTrackingPipeline` | Maritime chokepoint monitoring (Hormuz, Malacca, Suez) | Anomaly detection |
| `FlightTrackingPipeline` | ADS-B flight tracking, airspace restrictions | Military movement detection |
| `SatelliteImageryPipeline` | Site monitoring, change detection | Geospatial intelligence |
| `CommodityFlowsPipeline` | Supply chain risk, bottleneck detection | Cross-asset monitoring |
| `GlobalNewsPipeline` | GDELT + RSS collection, 10-category classification | Multi-source aggregation |
| `NewsSourcePipeline` | Credibility scoring (Reuters=0.95, RT=0.30) | Source reliability |

---

## DAG Orchestration

All 42 pipelines are wired into a master DAG with proper dependency ordering:

```
Ingestion ──► NLP ──► Knowledge Graph ──► World State ──► Features ──► Signals
  │                                                    │
  │                                                    ▼
  │                                          Training ──► Forecasting
  │                                                    │
  │                                                    ▼
  │                                               Backtesting
  │
  ├──► Alternative Data ──► Intelligence
  ├──► Event Similarity ──► Explainability
  └──► Daily Pipeline ──► Real-time Pipeline
```

---

## Core Architecture

```
pipelines/
├── __init__.py / _factory.py    # PipelineFactory + DAG wiring (42 pipelines)
│
├── core/                        # Foundation
│   ├── base.py                  # Pipeline / PipelineStage ABCs
│   ├── types.py                 # PipelineType, Event, Context, Outcome
│   └── state.py                 # PipelineState lifecycle
│
├── config/                      # Configuration
│   ├── settings.py              # Pydantic-settings (20+ env vars, MA_ prefix)
│   └── schemas.py               # StageSchema, DataSourceSchema, PipelineSchema
│
├── orchestration/               # Workflow Engine
│   ├── dag.py                   # NetworkX DiGraph with topological sort
│   ├── scheduler.py             # Cron-based periodic execution
│   ├── executor.py              # Thread-pooled async execution
│   ├── workflow.py              # DAG → pipeline mapping + topological execution
│   ├── triggers.py              # Cron, Event, Webhook triggers
│   └── monitor.py               # Run tracking + metrics
│
├── data_factory/                # Core processing stages
│   ├── collector.py             # GDELT, RSS, NewsAPI, Webhook collectors
│   ├── cleaner.py               # Deduplicator, Normalizer, Validator
│   ├── transformer.py           # SchemaMapper, EventBuilder
│   ├── enricher.py              # GeoEnricher, EntityLinker, TemporalEnricher
│   └── store.py                 # Postgres, Redis, S3, Qdrant stores
│
├── ingestion/                   # 4 ingestion pipelines
├── nlp/                         # 4 NLP pipelines
├── kg/                          # 4 Knowledge Graph pipelines
├── features/                    # 3 feature pipelines
├── training/                    # 3 training pipelines
├── forecasting/                 # 3 forecasting pipelines
├── backtesting/                 # 3 backtesting pipelines
├── streaming/                   # 4 streaming pipelines
├── similarity/                  # 5 similarity pipelines
├── explainability/              # 4 explainability pipelines
├── daily/                       # 7-stage daily pipeline
├── realtime/                    # 6-stage realtime pipeline
├── intelligence/                # 8 intelligence pipelines (conflict, economic, alternative, news)
│
└── config/                      # Settings + schemas
```

---

## Quick Start

```bash
# Install
pip install -e .

# Run the full DAG (all 42 pipelines)
python -c "
from pipelines import build_pipelines
factory = build_pipelines()
factory.run_all()
"

# Start the scheduler
python -c "
from pipelines import build_pipelines
factory = build_pipelines()
factory.start_scheduler()
"
```

---

## Configuration

All settings via environment variables with `MA_` prefix:

```bash
# Required
export MA_GDELT_API_KEY="your_key"
export MA_NEWSAPI_KEY="your_key"

# Optional (defaults to localhost)
export MA_QDRANT_URL="http://localhost:6333"
export MA_KAFKA_BOOTSTRAP="localhost:9092"
export MA_REDIS_URL="redis://localhost:6379/0"
export MA_POSTGRES_DSN="postgresql+asyncpg://user:pass@localhost:5432/marketatlas"
export MA_S3_ENDPOINT="http://localhost:9000"
export MA_MLFLOW_URI="http://localhost:5000"
```

---

## Design Principles

1. **Modular by construction** — Every pipeline is an independent stage-based unit
2. **DAG-native** — Dependencies are explicit, execution is topological
3. **Multi-trigger** — Cron, event, and webhook triggers on every pipeline
4. **Resilient by default** — Every stage has retries, timeouts, and error handling
5. **Observable** — Every run is tracked with duration, metrics, and outcomes
6. **Extensible** — New pipelines register in one place and auto-join the DAG
7. **Scale-ready** — From local laptop to distributed Kafka + S3 architecture
