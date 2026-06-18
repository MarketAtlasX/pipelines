from __future__ import annotations

import abc
import asyncio
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx

from pipelines.core.types import Event

logger = logging.getLogger(__name__)


class Collector(abc.ABC):
    source: str

    def __init__(self, source: str) -> None:
        self.source = source

    @abc.abstractmethod
    async def collect(self) -> AsyncIterator[Event]: ...

    async def collect_batch(self, max_events: int = 100) -> List[Event]:
        events = []
        async for event in self.collect():
            events.append(event)
            if len(events) >= max_events:
                break
        return events


class GDELTCollector(Collector):
    def __init__(self, api_url: str = "https://api.gdeltproject.org/api/v2/doc/doc", max_records: int = 250) -> None:
        super().__init__("gdelt")
        self.api_url = api_url
        self.max_records = max_records

    async def collect(self) -> AsyncIterator[Event]:
        params = {
            "query": "market sentiment OR geopolitical risk OR economic indicator",
            "mode": "ArtList",
            "format": "json",
            "maxrecords": self.max_records,
            "sort": "datedesc",
        }
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(self.api_url, params=params)
                resp.raise_for_status()
                data = resp.json()
                for article in data.get("articles", []):
                    yield Event(
                        source="gdelt",
                        type="news_article",
                        data={
                            "title": article.get("title"),
                            "url": article.get("url"),
                            "content": article.get("content"),
                            "source": article.get("source"),
                            "published": article.get("date"),
                            "categories": article.get("categories", []),
                            "locations": article.get("locations", []),
                            "tone": article.get("tone"),
                        },
                        metadata={
                            "gdelt_doc_id": article.get("id"),
                            "collected_at": datetime.utcnow().isoformat(),
                        },
                    )
            except httpx.HTTPError as e:
                logger.error("GDELT API error: %s", e)

    async def collect_conflict(self) -> AsyncIterator[Event]:
        params = {
            "query": "conflict OR war OR military OR violence OR protest",
            "mode": "ArtList",
            "format": "json",
            "maxrecords": self.max_records,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(self.api_url, params=params)
                resp.raise_for_status()
                data = resp.json()
                for article in data.get("articles", []):
                    yield Event(
                        source="gdelt",
                        type="conflict_intelligence",
                        data=article,
                    )
            except httpx.HTTPError as e:
                logger.error("GDELT conflict query error: %s", e)


class RSSCollector(Collector):
    def __init__(self, feeds: List[str]) -> None:
        super().__init__("rss")
        self.feeds = feeds

    async def collect(self) -> AsyncIterator[Event]:
        import xml.etree.ElementTree as ET

        async with httpx.AsyncClient(timeout=15) as client:
            for feed_url in self.feeds:
                try:
                    resp = await client.get(feed_url)
                    resp.raise_for_status()
                    root = ET.fromstring(resp.text)
                    ns = {"atom": "http://www.w3.org/2005/Atom"}
                    for entry in root.findall(".//item"):
                        yield Event(
                            source="rss",
                            type="news_article",
                            data={
                                "title": entry.findtext("title", ""),
                                "description": entry.findtext("description", ""),
                                "link": entry.findtext("link", ""),
                                "published": entry.findtext("pubDate", ""),
                                "feed": feed_url,
                            },
                        )
                except Exception as e:
                    logger.warning("RSS fetch error for %s: %s", feed_url, e)


class NewsAPICollector(Collector):
    def __init__(self, api_key: str) -> None:
        super().__init__("newsapi")
        self.api_key = api_key

    async def collect(self) -> AsyncIterator[Event]:
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": "markets OR economy OR geopolitics OR trade",
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 100,
            "apiKey": self.api_key,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                for article in data.get("articles", []):
                    yield Event(
                        source="newsapi",
                        type="news_article",
                        data={
                            "title": article.get("title"),
                            "description": article.get("description"),
                            "content": article.get("content"),
                            "url": article.get("url"),
                            "source_name": article.get("source", {}).get("name"),
                            "published": article.get("publishedAt"),
                        },
                    )
            except httpx.HTTPError as e:
                logger.error("NewsAPI error: %s", e)


class WebhookCollector(Collector):
    def __init__(self) -> None:
        super().__init__("webhook")
        self._events: asyncio.Queue[Event] = asyncio.Queue()

    async def receive(self, payload: Dict[str, Any]) -> Event:
        event = Event(source="webhook", type="incoming", data=payload)
        await self._events.put(event)
        return event

    async def collect(self) -> AsyncIterator[Event]:
        while True:
            event = await self._events.get()
            yield event
