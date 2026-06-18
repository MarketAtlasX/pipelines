from __future__ import annotations

import logging

from pipelines.core.base import PipelineStage
from pipelines.core.types import Context, Event
from pipelines.data_factory.enricher import GeoEnricher, EntityLinker, TemporalEnricher

logger = logging.getLogger(__name__)


class DailyEnrichmentStage(PipelineStage):
    def __init__(self) -> None:
        super().__init__("daily_enrichment")
        self.enrichers = [GeoEnricher(), EntityLinker(), TemporalEnricher()]

    async def run(self, event: Event, context: Context) -> Event:
        events = event.data.get("deduped_events", [event.model_dump()])
        enriched = []

        for e in events:
            if not isinstance(e, dict):
                continue
            ev = Event(**e)
            for enricher in self.enrichers:
                ev = await enricher.enrich(ev)
            enriched.append(ev.model_dump())

        event.data["enriched_events"] = enriched
        event.data["enrichment_count"] = len(enriched)
        logger.info("Enriched %d events", len(enriched))
        return event
