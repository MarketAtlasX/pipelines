from __future__ import annotations

from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class StageType(str, Enum):
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    ENRICH = "enrich"
    DEDUPLICATE = "deduplicate"
    EMBED = "embed"
    SEARCH = "search"
    ANALYZE = "analyze"
    TRAIN = "train"
    PREDICT = "predict"
    EVALUATE = "evaluate"
    NOTIFY = "notify"


class StageSchema(BaseModel):
    name: str
    type: StageType
    config: Dict[str, Any] = Field(default_factory=dict)
    retries: int = 3
    timeout_seconds: int = 300
    dependencies: List[str] = Field(default_factory=list)


class DataSourceSchema(BaseModel):
    name: str
    type: str
    connection: Dict[str, Any] = Field(default_factory=dict)
    data_schema: Dict[str, Any] = Field(default_factory=dict, alias="schema")
    credentials: Optional[str] = None


class ModelSchema(BaseModel):
    name: str
    framework: str = "sklearn"
    parameters: Dict[str, Any] = Field(default_factory=dict)
    artifact_path: Optional[str] = None
    version: Optional[str] = None


class TriggerSchema(BaseModel):
    type: str
    schedule: Optional[str] = None
    event_type: Optional[str] = None
    cron: Optional[str] = None


class PipelineSchema(BaseModel):
    name: str
    version: str = "1.0.0"
    description: Optional[str] = None
    stages: List[StageSchema]
    triggers: List[TriggerSchema] = Field(default_factory=list)
    sources: List[DataSourceSchema] = Field(default_factory=list)
    models: List[ModelSchema] = Field(default_factory=list)
    tags: Dict[str, str] = Field(default_factory=dict)
    max_concurrency: int = 1
    timeout: timedelta = timedelta(hours=1)
