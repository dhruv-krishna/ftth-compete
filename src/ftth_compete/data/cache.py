"""SQLite-backed key-value cache for API responses.

Keyed by (source, key). TTLs vary by source: Google Places ratings get
30 days; Census ACS responses are effectively immutable per vintage so
get no TTL (kept until manually cleared by `make refresh`).
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Iterator

from ..config import get_settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cache (
    source     TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    PRIMARY KEY (source, key)
);
CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache(expires_at);
"""


def _resolve_path(db_path: Path | None) -> Path:
    if db_path is not None:
        return db_path
    return get_settings().cache_db_path


@contextmanager
def _conn(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    path = _resolve_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_SCHEMA)
        yield conn
        conn.commit()
    finally:
        conn.close()


def get(source: str, key: str, *, db_path: Path | None = None) -> bytes | None:
    """Return cached value or None if missing/expired."""
    now = int(time.time())
    with _conn(db_path) as conn:
        row = conn.execute(
            "SELECT value, expires_at FROM cache WHERE source=? AND key=?",
            (source, key),
        ).fetchone()
    if row is None:
        return None
    value, expires_at = row
    if expires_at is not None and expires_at < now:
        return None
    return value


def put(
    source: str,
    key: str,
    value: bytes,
    ttl: timedelta | None = None,
    *,
    db_path: Path | None = None,
) -> None:
    """Store value. ttl=None means no expiration."""
    now = int(time.time())
    expires_at = None if ttl is None else now + int(ttl.total_seconds())
    with _conn(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO cache(source, key, value, created_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (source, key, value, now, expires_at),
        )


def clear(source: str | None = None, *, db_path: Path | None = None) -> int:
    """Clear cache entries. Returns count of rows removed."""
    with _conn(db_path) as conn:
        if source is None:
            cur = conn.execute("DELETE FROM cache")
        else:
            cur = conn.execute("DELETE FROM cache WHERE source=?", (source,))
        return cur.rowcount or 0
