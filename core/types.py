from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PipelineType(str, Enum):
    INGESTION = "ingestion"
    NLP = "nlp"
    KNOWLEDGE_GRAPH = "knowledge_graph"
    FEATURE_ENGINEERING = "feature_engineering"
    TRAINING = "training"
    FORECASTING = "forecasting"
    BACKTESTING = "backtesting"
    STREAMING = "streaming"
    DAILY = "daily"
    REALTIME = "realtime"
    SIMILARITY = "similarity"
    EXPLAINABILITY = "explainability"
    CONFLICT_INTEL = "conflict_intelligence"
    ECONOMIC_INTEL = "economic_intelligence"
    ALTERNATIVE_INTEL = "alternative_intelligence"
    GLOBAL_NEWS = "global_news"


class PipelineStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class Event(BaseModel):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    source: str
    type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def enrich(self, key: str, value: Any) -> None:
        self.data[key] = value

    def tag(self, key: str, value: Any) -> None:
        self.metadata[key] = value


class Context(BaseModel):
    run_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    pipeline: str
    pipeline_type: PipelineType
    started_at: datetime = Field(default_factory=datetime.utcnow)
    params: Dict[str, Any] = Field(default_factory=dict)
    state: Dict[str, Any] = Field(default_factory=dict)
    tags: Dict[str, str] = Field(default_factory=dict)


class Outcome(BaseModel):
    context: Context
    status: PipelineStatus
    events: List[Event] = Field(default_factory=list)
    artifacts: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, float] = Field(default_factory=dict)
    error: Optional[str] = None
    completed_at: Optional[datetime] = None
