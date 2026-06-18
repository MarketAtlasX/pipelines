# MarketAtlas Pipelines

A modular, event-driven data pipeline factory for geopolitical risk intelligence, market impact analysis, and alternative data processing. This is the **data factory** — it collects, cleans, transforms, enriches, and stores event data at scale.

## Architecture

```
                    ┌────────────────────────────────────────────┐
                    │           PipelineFactory (Brain)          │
                    │  ┌──────────┐  ┌─────────┐  ┌──────────┐ │
                    │  │  DAG     │  │Scheduler│  │ Executor │ │
                    │  │Orchestrat│  │  Cron   │  │  Async   │ │
                    │  └──────────┘  └─────────┘  └──────────┘ │
                    └────────────────────────────────────────────┘
                                      │
        ┌──────────────┬──────────────┼──────────────┬──────────────┐
        ▼              ▼              ▼              ▼              ▼
  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │Ingestion │  │   NLP    │  │    KG    │  │ Features │  │  Intel   │
  │ GDElT    │  │ Embedding │  │ Nodes    │  │ Engineer │  │ Conflict │
  │ NewsAPI  │  │ Entities  │  │ Edges    │  │ Signals  │  │ Economic │
  │ RSS      │  │ Sentiment │  │ Queries  │  │ Store    │  │ Alt Data │
  │ Webhooks │  │ Summarize │  │ Graph    │  │          │  │ News     │
  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘
        │              │              │              │              │
        ▼              ▼              ▼              ▼              ▼
  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ Training │  │Forecasting│  │Backtest  │  │Similarity│  │Explain   │
  │ Models   │  │ Prophet   │  │ Engine   │  │ Qdrant   │  │ SHAP     │
  │ Registry │  │ LSTM      │  │ Metrics  │  │ Embed    │  │ Paths    │
  │ Eval     │  │ Ensemble  │  │ Report   │  │ Match    │  │ Analogs  │
  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘
        │              │              │              │              │
        └──────────────┴──────────────┼──────────────┴──────────────┘
                                     ▼
                          ┌─────────────────────┐
                          │  Streaming / Realtime│
                          │  Kafka → Process →   │
                          │  WebSocket → Sink    │
                          └─────────────────────┘
```

## Quick Start

```python
from pipelines import build_pipelines

factory = build_pipelines()

# Run a single pipeline
outcome = await factory.run("event_similarity", event=my_event)

# Run all pipelines in DAG order
results = await factory.run_all()

# Start the scheduler for cron-triggered pipelines
await factory.start_scheduler()
```

## Pipeline Catalog (42 pipelines)

### Data Factory (`data_factory/`)
The foundational layer — every pipeline flows through these stages:

| Module | Purpose |
|--------|---------|
| **Collector** | Fetches raw data from GDELT, NewsAPI, RSS feeds, webhooks |
| **Cleaner** | Deduplicates, normalizes, and validates events |
| **Transformer** | Maps schemas and builds unified event objects |
| **Enricher** | Adds geo-location, entity linking, temporal context |
| **Store** | Abstract storage layer (Postgres, Redis, S3, Qdrant) |

### Ingestion (`ingestion/`)
- **GDELT Pipeline** — fetches from GDELT Project API (global news & conflicts)
- **NewsAPI Pipeline** — pulls from NewsAPI.org
- **RSS Pipeline** — consumes RSS/Atom feeds
- **Webhook Pipeline** — receives real-time webhook payloads

### NLP Enrichment (`nlp/`)
- **Embedding Pipeline** — generates vector embeddings via `sentence-transformers` (all-MiniLM-L6-v2)
- **Entity Extraction Pipeline** — detects countries, organizations, and key entities
- **Sentiment Pipeline** — lexicon-based positive/negative/neutral scoring
- **Summarization Pipeline** — extractive summarization of articles

### Knowledge Graph (`kg/`)
- **Node Builder Pipeline** — creates graph nodes for events, countries, organizations
- **Edge Builder Pipeline** — infers relationships (MENTIONS, AFFECTS, LOCATED_IN)
- **Graph Build Pipeline** — constructs a NetworkX `DiGraph` with centrality analysis
- **Graph Query Pipeline** — shortest path, neighbors, subgraph, and impact path queries

### Feature Engineering (`features/`)
- **Feature Extraction Pipeline** — builds numeric feature vectors (sentiment, entity density, temporal signals)
- **Signal Generation Pipeline** — generates market signals (bearish/bullish, risk levels)
- **Feature Store Pipeline** — persists features to Redis/S3

### Training (`training/`)
- **Training Pipeline** — trains a RandomForest classifier on event features
- **Model Registry Pipeline** — manages model versions and metadata
- **Evaluation Pipeline** — accuracy, classification report, confusion matrix

### Forecasting (`forecasting/`)
- **Forecaster Pipeline** — projects sentiment trends 30 days forward with confidence bounds
- **Forecast Models Pipeline** — runs Prophet-like trend decomposition and LSTM simulation
- **Ensemble Pipeline** — weighted ensemble combining all forecast models

### Backtesting (`backtesting/`)
- **Engine Pipeline** — simulates trades based on signals, computes P&L
- **Metrics Pipeline** — Sharpe ratio, max drawdown, win rate
- **Reporting Pipeline** — generates structured backtest reports

### Streaming (`streaming/`)
- **Kafka Pipeline** — consumes/produces events from Kafka topics
- **Stream Processor Pipeline** — real-time transform and filter
- **WebSocket Pipeline** — broadcasts pipeline updates to connected clients
- **Stream Sink Pipeline** — persists streaming results to data stores

### Intelligence (`intelligence/`)

| Pipeline | Source | Purpose |
|----------|--------|---------|
| **Conflict Intelligence** | GDELT + NLP | Escalation detection, active conflict zones, trigger monitoring |
| **Economic Intelligence** | News text | Indicator extraction (GDP, inflation, employment, trade) |
| **AIS Vessel Tracking** | AIS data | Maritime chokepoint monitoring, anomaly detection |
| **Flight Tracking** | ADS-B | Airspace restrictions, military movement detection |
| **Satellite Imagery** | Satellite | Site monitoring, change detection |
| **Commodity Flows** | Trade data | Supply chain risk, bottleneck detection |
| **Global News** | Multi-source | Category classification, source credibility scoring |

### Event Similarity (`similarity/`)
The specific pipeline you asked for:

```
New Event → Embedding Model → Qdrant Search → Top 20 Similar Events
→ Market Outcome Retrieval → Store Similarity Links
```

### Explainability (`explainability/`)

```
Prediction → SHAP → Graph Path Extraction → Historical Analogs
→ Explanation Generator
```

### End-to-End Pipelines

**Daily Pipeline** (`daily/`)
```
GDELT → Deduplication → Enrichment → Graph Update → Feature Generation
→ Signal Generation → Store Results
```

**Real-Time Pipeline** (`realtime/`)
```
New Event → Kafka → Embedding → Similarity Search → Impact Analysis
→ Graph Update → WebSocket Push
```

## Orchestration

The **brain** lives in `_factory.py`:

- **DAG** (`orchestration/dag.py`) — Directed Acyclic Graph built on NetworkX for dependency resolution
- **WorkflowBuilder** (`orchestration/workflow.py`) — maps DAG nodes to pipeline instances, executes in topological layers
- **Scheduler** (`orchestration/scheduler.py`) — cron-based periodic execution
- **Executor** (`orchestration/executor.py`) — thread-pooled async execution with error handling
- **Triggers** (`orchestration/triggers.py`) — CronTrigger, EventTrigger, WebhookTrigger
- **Monitor** (`orchestration/monitor.py`) — tracks every run with status, duration, and metrics

### Default Schedule

| Pipeline | Cadence |
|----------|---------|
| Daily | Every 24h |
| Global News | Every 1h |
| AIS Tracking | Every 30m |
| Flight Tracking | Every 30m |

## Configuration

All settings are environment-driven via `PipelineSettings` (pydantic-settings).

```bash
export MA_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export MA_QDRANT_URL="http://localhost:6333"
export MA_EMBEDDING_MODEL="all-MiniLM-L6-v2"
export MA_POSTGRES_URL="postgresql+asyncpg://user:pass@localhost:5432/marketatlas"
```

Or create a `.env` file:

```ini
MA_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MA_KAFKA_INPUT_TOPIC=raw-events
MA_QDRANT_URL=http://localhost:6333
MA_EMBEDDING_MODEL=all-MiniLM-L6-v2
MA_LOG_LEVEL=INFO
```

## Data Flow

```
                     Collect
                        ↓
                      Clean
                        ↓
                    Transform
                        ↓
                     Enrich
                        ↓
                 ┌──── Store ────┐
                 │               │
            PostgreSQL         S3
            (structured)    (archive)
                 │               │
              Redis           Qdrant
            (features)     (vectors)
```

Every event flows through the **data factory** pipeline (`data_factory/`), which ensures consistency before any downstream processing.

## Project Structure

```
pipelines/
├── __init__.py          # Public API: PipelineFactory, build_pipelines
├── _factory.py          # Brain: registers all 42 pipelines, builds DAG
├── pyproject.toml       # Python project config & dependencies
├── .gitignore
├── .env                 # Environment variables (optional)
│
├── core/                # Base types, Pipeline ABC, Context, Event, State
├── config/              # Pydantic settings & pipeline schemas
├── orchestration/       # DAG, Scheduler, Executor, Workflow, Triggers, Monitor
├── data_factory/        # Collector, Cleaner, Transformer, Enricher, Store
│
├── ingestion/           # GDELT, NewsAPI, RSS, Webhooks
├── nlp/                 # Embedding, Entities, Sentiment, Summarization
├── kg/                  # Knowledge Graph: nodes, edges, graph, queries
├── features/            # Engineering, Signals, Feature Store
├── training/            # Trainer, Model Registry, Evaluation
├── forecasting/         # Forecaster, Prophet/LSTM, Ensemble
├── backtesting/         # Engine, Risk Metrics, Reporting
├── streaming/           # Kafka, Stream Processor, WebSocket, Sink
│
├── intelligence/        # Conflict, Economic, Alternative, Global News
│   ├── alternative/     # AIS, Flights, Satellite, Commodities
│   └── news/            # Global News, Source Credibility
│
├── similarity/          # Event Similarity: Embed → Qdrant → Match → Store
├── explainability/      # SHAP → Graph Paths → Analogs → Generator
├── daily/               # Full daily run: GDELT → ... → Store
└── realtime/            # Full real-time: Kafka → ... → WebSocket
```
