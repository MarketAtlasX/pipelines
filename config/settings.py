from __future__ import annotations

from typing import Dict, List, Optional

from pydantic_settings import BaseSettings


class PipelineSettings(BaseSettings):
    model_config = {"env_prefix": "MA_", "env_nested_delimiter": "__"}

    project: str = "marketatlas"
    environment: str = "development"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_input_topic: str = "raw-events"
    kafka_output_topic: str = "enriched-events"
    kafka_consumer_group: str = "pipelines"
    kafka_auto_offset_reset: str = "earliest"

    # Qdrant
    qdrant_url: str = "http://localhost:6333"
    qdrant_collection: str = "event-embeddings"
    qdrant_vector_size: int = 384

    # Embedding
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_device: str = "cpu"
    embedding_batch_size: int = 64

    # Graph
    graph_db_url: str = "neo4j://localhost:7687"
    graph_db_user: str = "neo4j"
    graph_db_password: str = "password"

    # Storage
    redis_url: str = "redis://localhost:6379"
    postgres_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketatlas"
    s3_bucket: str = "marketatlas-data"
    s3_region: str = "us-east-1"

    # Pipeline
    max_parallel_stages: int = 4
    stage_timeout_seconds: int = 300
    retry_max_attempts: int = 3
    retry_delay_seconds: int = 5

    # WebSocket
    ws_host: str = "0.0.0.0"
    ws_port: int = 8765

    # MLflow
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_experiment: str = "marketatlas"

    # Monitoring
    log_level: str = "INFO"
    sentry_dsn: Optional[str] = None
    statsd_host: Optional[str] = None
    statsd_port: int = 8125

    # GDELT
    gdelt_api_url: str = "https://api.gdeltproject.org/api/v2/doc/doc"
    gdelt_max_records: int = 250

    # Data sources
    newsapi_key: Optional[str] = None
    rss_feeds: List[str] = [
        "http://rss.cnn.com/rss/edition.rss",
        "http://feeds.bbci.co.uk/news/rss.xml",
        "https://feeds.reuters.com/reuters/topNews",
    ]

    # Feature store
    feature_store_backend: str = "redis"
    signal_window_days: int = 7

    # Forecasting
    forecast_horizon_days: int = 30
    forecast_models: List[str] = ["prophet", "lstm", "transformer"]
