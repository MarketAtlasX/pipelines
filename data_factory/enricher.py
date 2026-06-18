from __future__ import annotations

import abc
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from pipelines.core.types import Event

logger = logging.getLogger(__name__)


class Enricher(abc.ABC):
    @abc.abstractmethod
    async def enrich(self, event: Event) -> Event: ...


class GeoEnricher(Enricher):
    def __init__(self, api_url: str = "https://nominatim.openstreetmap.org/search") -> None:
        self.api_url = api_url

    async def enrich(self, event: Event) -> Event:
        text = f"{event.data.get('title', '')} {event.data.get('content', '')}"
        location_patterns = re.findall(
            r"\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\b", text
        )
        seen = set()
        locations = []
        for loc in location_patterns:
            if loc not in seen and len(loc) > 2:
                seen.add(loc)
                locations.append({"name": loc, "confidence": 0.5})

        if locations:
            event.data["locations"] = locations
            event.data["has_geo"] = True
        return event


class EntityLinker(Enricher):
    async def enrich(self, event: Event) -> Event:
        text = f"{event.data.get('title', '')} {event.data.get('content', '')}"
        entities = {
            "organizations": self._extract_orgs(text),
            "people": self._extract_people(text),
            "countries": self._extract_countries(text),
            "events": [],
        }
        event.data["entities"] = entities
        event.data["entity_count"] = sum(len(v) for v in entities.values())
        return event

    def _extract_orgs(self, text: str) -> List[str]:
        patterns = [
            r"\b(?:UN|NATO|EU|OPEC|IMF|World Bank|Fed|ECB)\b",
            r"\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)*\s(?:Inc|Corp|LLC|Group|Bank|Fund|Agency)\b",
        ]
        found = []
        for p in patterns:
            found.extend(re.findall(p, text))
        return list(set(found))

    def _extract_people(self, text: str) -> List[str]:
        return []

    def _extract_countries(self, text: str) -> List[str]:
        countries = [
            "USA", "China", "Russia", "Ukraine", "Iran", "Israel", "India",
            "Germany", "France", "UK", "Japan", "Saudi Arabia", "Turkey",
            "North Korea", "Syria", "Afghanistan", "Venezuela", "Brazil",
        ]
        found = []
        for c in countries:
            if c.lower() in text.lower():
                found.append(c)
        return found


class TemporalEnricher(Enricher):
    async def enrich(self, event: Event) -> Event:
        published = event.data.get("published")
        if published:
            try:
                parsed = datetime.fromisoformat(published.replace("Z", "+00:00"))
                event.data["published_parsed"] = parsed.isoformat()
                event.data["published_date"] = parsed.date().isoformat()
                event.data["published_hour"] = parsed.hour
                event.data["day_of_week"] = parsed.strftime("%A")
            except (ValueError, AttributeError):
                event.data["published_parsed"] = datetime.now(timezone.utc).isoformat()
        event.data["ingested_at"] = datetime.utcnow().isoformat()
        return event


class PipelineEnricher(Enricher):
    def __init__(self, enrichers: Optional[List[Enricher]] = None) -> None:
        self.enrichers = enrichers or [GeoEnricher(), EntityLinker(), TemporalEnricher()]

    def add(self, enricher: Enricher) -> PipelineEnricher:
        self.enrichers.append(enricher)
        return self

    async def enrich(self, event: Event) -> Event:
        current = event
        for enricher in self.enrichers:
            current = await enricher.enrich(current)
        return current
