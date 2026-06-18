from __future__ import annotations

import abc
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipelines.core.types import Event

logger = logging.getLogger(__name__)


class Transformer(abc.ABC):
    @abc.abstractmethod
    async def transform(self, event: Event) -> Event: ...


class SchemaMapper(Transformer):
    def __init__(self, mapping: Dict[str, str]) -> None:
        self.mapping = mapping

    async def transform(self, event: Event) -> Event:
        mapped: Dict[str, Any] = {}
        for target, source in self.mapping.items():
            if source in event.data:
                mapped[target] = event.data[source]
        event.data.update(mapped)
        return event


class EventBuilder(Transformer):
    def __init__(self, template: Optional[Dict[str, Any]] = None) -> None:
        self.template = template or {}

    async def transform(self, event: Event) -> Event:
        built = dict(self.template)
        built["original_type"] = event.type
        built["source"] = event.source
        built["timestamp"] = event.timestamp.isoformat()
        built["content"] = event.data.get("content") or event.data.get("description") or ""
        built["title"] = event.data.get("title") or ""
        built["url"] = event.data.get("url") or ""
        built["source_name"] = event.data.get("source_name") or event.source
        built["language"] = event.data.get("language", "en")
        event.data.update(built)
        return event


class PipelineTransformer(Transformer):
    def __init__(self, transformers: Optional[List[Transformer]] = None) -> None:
        self.transformers = transformers or []

    def add(self, transformer: Transformer) -> PipelineTransformer:
        self.transformers.append(transformer)
        return self

    async def transform(self, event: Event) -> Event:
        current = event
        for transformer in self.transformers:
            current = await transformer.transform(current)
        return current
