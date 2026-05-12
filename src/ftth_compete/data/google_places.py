"""Google Places API (New) — provider rating lookups.

`rating` and `userRatingCount` are Enterprise-tier fields with only 1,000
free events/month as of Mar 2025 (https://developers.google.com/maps/billing-and-pricing/march-2025).
Strategy: 30-day TTL on every cache layer, batch per market, FieldMask
limited to the rating fields only.

Per provider, two API calls on cold cache:
    1. Text Search: "Provider Name internet in City, ST" -> place_id
    2. Place Details for that place_id -> rating + userRatingCount

Both responses cached for 30 days. After warmup, repeated market lookups
of the same provider/market are free (no API call).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Final

import httpx

from ..config import get_settings
from . import cache

log = logging.getLogger(__name__)

API_BASE: Final = "https://places.googleapis.com/v1"
TTL: Final = timedelta(days=30)
CACHE_SEARCH: Final = "google_places_search"  # query -> place_id (or empty)
CACHE_DETAILS: Final = "google_places_details"  # place_id -> rating json (or empty)


@dataclass(frozen=True)
class Rating:
    """A Google Places rating snapshot."""

    place_id: str
    display_name: str
    rating: float | None  # 1.0-5.0 average; None if no rating yet
    user_rating_count: int | None
    place_url: str | None = None  # https://maps.google.com/?cid=... for context

    def to_dict(self) -> dict[str, object]:
        return {
            "place_id": self.place_id,
            "display_name": self.display_name,
            "rating": self.rating,
            "user_rating_count": self.user_rating_count,
            "place_url": self.place_url,
        }


def _api_key() -> str:
    s = get_settings()
    if not s.google_places_key:
        raise RuntimeError(
            "GOOGLE_PLACES_KEY not set in .env. Get one at "
            "https://console.cloud.google.com/apis/credentials and enable "
            "Places API (New) on the project."
        )
    return s.google_places_key


def _text_search(query: str) -> str | None:
    """Find first place_id matching `query`. Returns None if no match.

    Caches both hits and misses for 30 days (empty bytes = negative cache).
    """
    cache_key = query.lower()
    cached = cache.get(CACHE_SEARCH, cache_key)
    if cached is not None:
        v = cached.decode("utf-8")
        return v or None

    url = f"{API_BASE}/places:searchText"
    headers = {
        "X-Goog-Api-Key": _api_key(),
        "X-Goog-FieldMask": "places.id,places.displayName",
        "Content-Type": "application/json",
    }
    body: dict[str, object] = {"textQuery": query, "pageSize": 1}
    log.info("Places searchText: %r", query)
    r = httpx.post(url, headers=headers, json=body, timeout=30.0)
    if r.status_code == 401:
        raise RuntimeError("Google Places returned 401 - check GOOGLE_PLACES_KEY")
    r.raise_for_status()
    data = r.json()
    places = data.get("places") or []
    if not places:
        cache.put(CACHE_SEARCH, cache_key, b"", ttl=TTL)
        return None
    place_id = str(places[0]["id"])
    cache.put(CACHE_SEARCH, cache_key, place_id.encode("utf-8"), ttl=TTL)
    return place_id


def _place_details(place_id: str) -> Rating | None:
    """Fetch rating + userRatingCount for a place_id. Caches 30 days."""
    cached = cache.get(CACHE_DETAILS, place_id)
    if cached is not None:
        if not cached:
            return None  # negative cache
        d = json.loads(cached.decode("utf-8"))
        return Rating(**d)

    url = f"{API_BASE}/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": _api_key(),
        "X-Goog-FieldMask": "id,displayName,rating,userRatingCount,googleMapsUri",
    }
    log.info("Places details: %s", place_id)
    r = httpx.get(url, headers=headers, timeout=30.0)
    if r.status_code == 401:
        raise RuntimeError("Google Places returned 401 - check GOOGLE_PLACES_KEY")
    r.raise_for_status()
    data = r.json()

    rating = Rating(
        place_id=str(data.get("id") or place_id),
        display_name=(data.get("displayName") or {}).get("text") or "",
        rating=data.get("rating"),
        user_rating_count=data.get("userRatingCount"),
        place_url=data.get("googleMapsUri"),
    )
    cache.put(CACHE_DETAILS, place_id, json.dumps(rating.to_dict()).encode("utf-8"), ttl=TTL)
    return rating


def get_rating(provider_name: str, market_label: str) -> Rating | None:
    """Look up Google Places rating for a provider in a market.

    `market_label` is "City, ST" form (e.g. "Evans, CO"). Used for geographic
    context in the Text Search query — without it, we'd match HQ locations
    in random cities rather than the local service.
    """
    query = f"{provider_name} internet {market_label}"
    place_id = _text_search(query)
    if place_id is None:
        return None
    return _place_details(place_id)


def batch_get_ratings(
    provider_names: list[str], market_label: str
) -> dict[str, Rating | None]:
    """Look up ratings for multiple providers in one market.

    Best-effort: per-provider failures are logged and recorded as None so
    one bad lookup doesn't kill the whole batch.
    """
    out: dict[str, Rating | None] = {}
    for name in provider_names:
        try:
            out[name] = get_rating(name, market_label)
        except Exception as exc:  # noqa: BLE001
            log.exception("Places lookup failed for %r", name)
            out[name] = None
            _ = exc  # surface in logs
    return out
