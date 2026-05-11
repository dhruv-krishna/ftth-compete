"""Lightweight visitor/event log for the personal-only admin sidecar.

SQLite-backed (`FTTH_DATA_DIR/visitors.db`). On HF Spaces free tier the
DB file lives on ephemeral disk — it's wiped on container restart. That
is intentional: no persistent PII storage. If you want longer retention
attach HF persistent storage ($5/mo) or pipe events to an external sink.

Privacy posture:
- IPs are hashed (first 8 chars of SHA-256) before storage, never raw.
- User-Agent is truncated to 200 chars.
- Event payloads are caller-controlled — keep PII out of them.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)
_lock = threading.Lock()
_db_path: Path | None = None


def _get_db_path() -> Path:
    """Resolve the visitor-log DB path under FTTH_DATA_DIR. Cached."""
    global _db_path
    if _db_path is None:
        from ftth_compete.config import get_settings
        settings = get_settings()
        _db_path = Path(settings.data_dir) / "visitors.db"
        _db_path.parent.mkdir(parents=True, exist_ok=True)
    return _db_path


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            session_id TEXT,
            ip_hash TEXT,
            ua TEXT,
            kind TEXT NOT NULL,
            payload TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts DESC)")
    conn.commit()


def _hash_ip(ip: str | None) -> str | None:
    if not ip:
        return None
    return hashlib.sha256(ip.encode("utf-8")).hexdigest()[:8]


def record(
    kind: str,
    payload: dict | None = None,
    *,
    session_id: str | None = None,
    ip: str | None = None,
    ua: str | None = None,
) -> None:
    """Best-effort event insert. Failures are logged and swallowed so
    analytics outages never break the app."""
    try:
        ts = datetime.now(timezone.utc).isoformat()
        ip_hash = _hash_ip(ip)
        payload_json = json.dumps(payload or {}, default=str)
        with _lock:
            conn = sqlite3.connect(_get_db_path())
            try:
                _init_db(conn)
                conn.execute(
                    "INSERT INTO events (ts, session_id, ip_hash, ua, kind, payload) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (ts, session_id, ip_hash, (ua or "")[:200], kind, payload_json),
                )
                conn.commit()
            finally:
                conn.close()
    except Exception:
        log.exception("analytics.record failed (non-fatal)")


def recent(limit: int = 300) -> list[dict]:
    try:
        with _lock:
            conn = sqlite3.connect(_get_db_path())
            try:
                _init_db(conn)
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT id, ts, session_id, ip_hash, ua, kind, payload "
                    "FROM events ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()
    except Exception:
        log.exception("analytics.recent failed (non-fatal)")
        return []


def summary() -> dict:
    """Quick aggregates for the admin page header."""
    try:
        with _lock:
            conn = sqlite3.connect(_get_db_path())
            try:
                _init_db(conn)
                conn.row_factory = sqlite3.Row
                today = datetime.now(timezone.utc).date().isoformat()
                sessions_today = conn.execute(
                    "SELECT COUNT(DISTINCT session_id) AS n FROM events WHERE ts >= ?",
                    (today,),
                ).fetchone()["n"]
                unique_today = conn.execute(
                    "SELECT COUNT(DISTINCT ip_hash) AS n FROM events "
                    "WHERE ts >= ? AND ip_hash IS NOT NULL",
                    (today,),
                ).fetchone()["n"]
                by_kind = conn.execute(
                    "SELECT kind, COUNT(*) AS n FROM events "
                    "WHERE ts >= ? GROUP BY kind ORDER BY n DESC",
                    (today,),
                ).fetchall()
                total = conn.execute(
                    "SELECT COUNT(*) AS n FROM events"
                ).fetchone()["n"]
                return {
                    "sessions_today": sessions_today,
                    "unique_ips_today": unique_today,
                    "total_events": total,
                    "by_kind_today": [(r["kind"], r["n"]) for r in by_kind],
                }
            finally:
                conn.close()
    except Exception:
        log.exception("analytics.summary failed (non-fatal)")
        return {
            "sessions_today": 0,
            "unique_ips_today": 0,
            "total_events": 0,
            "by_kind_today": [],
        }
