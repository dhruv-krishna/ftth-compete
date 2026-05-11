"""Tests for ftth_compete.data.cache."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from ftth_compete.data import cache


def test_put_and_get_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    cache.put("test_src", "k1", b"hello", db_path=db)
    assert cache.get("test_src", "k1", db_path=db) == b"hello"


def test_get_missing_returns_none(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    assert cache.get("test_src", "nonexistent", db_path=db) is None


def test_ttl_expiry(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    # ttl=0 means already expired
    cache.put("test_src", "k1", b"hello", ttl=timedelta(seconds=-1), db_path=db)
    assert cache.get("test_src", "k1", db_path=db) is None


def test_no_ttl_persists(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    cache.put("test_src", "k1", b"hello", db_path=db)
    # Should still be there
    assert cache.get("test_src", "k1", db_path=db) == b"hello"


def test_overwrite_same_key(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    cache.put("test_src", "k1", b"first", db_path=db)
    cache.put("test_src", "k1", b"second", db_path=db)
    assert cache.get("test_src", "k1", db_path=db) == b"second"


def test_isolation_by_source(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    cache.put("src_a", "k", b"a_val", db_path=db)
    cache.put("src_b", "k", b"b_val", db_path=db)
    assert cache.get("src_a", "k", db_path=db) == b"a_val"
    assert cache.get("src_b", "k", db_path=db) == b"b_val"


def test_clear_by_source(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    cache.put("src_a", "k", b"a_val", db_path=db)
    cache.put("src_b", "k", b"b_val", db_path=db)
    removed = cache.clear("src_a", db_path=db)
    assert removed == 1
    assert cache.get("src_a", "k", db_path=db) is None
    assert cache.get("src_b", "k", db_path=db) == b"b_val"


def test_clear_all(tmp_path: Path) -> None:
    db = tmp_path / "cache.db"
    cache.put("src_a", "k1", b"v1", db_path=db)
    cache.put("src_b", "k2", b"v2", db_path=db)
    removed = cache.clear(db_path=db)
    assert removed == 2
    assert cache.get("src_a", "k1", db_path=db) is None
    assert cache.get("src_b", "k2", db_path=db) is None
