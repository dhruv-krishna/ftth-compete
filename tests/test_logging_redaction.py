"""Tests for the secret-redaction logging filter."""

from __future__ import annotations

import logging

import ftth_compete  # noqa: F401 — triggers _install_log_redactor


def test_redactor_strips_census_api_key(caplog) -> None:
    log = logging.getLogger("ftth_compete.test")
    with caplog.at_level(logging.INFO, logger="ftth_compete.test"):
        log.info(
            "GET https://api.census.gov/data/2024/acs/acs5"
            "?get=B01003_001E&for=tract:*&in=state:08&key=secretkey1234"
        )
    text = caplog.text
    assert "secretkey1234" not in text
    assert "key=<REDACTED>" in text


def test_redactor_strips_fcc_hash_value(caplog) -> None:
    log = logging.getLogger("ftth_compete.test")
    with caplog.at_level(logging.INFO, logger="ftth_compete.test"):
        log.info("hash_value=abc123token GET /downloads/listAsOfDates")
    text = caplog.text
    assert "abc123token" not in text
    assert "hash_value=<REDACTED>" in text


def test_redactor_leaves_other_text_alone(caplog) -> None:
    log = logging.getLogger("ftth_compete.test")
    with caplog.at_level(logging.INFO, logger="ftth_compete.test"):
        log.info("Resolving Evans, CO via TIGER...")
        log.info("BDC ingest done: 117 CSVs -> /path/to/state=08.parquet")
    text = caplog.text
    assert "Resolving Evans" in text
    assert "117 CSVs" in text
    assert "REDACTED" not in text


def test_redactor_handles_multiple_params(caplog) -> None:
    log = logging.getLogger("ftth_compete.test")
    with caplog.at_level(logging.INFO, logger="ftth_compete.test"):
        log.info("url?key=k1&api_key=k2&token=t3&for=tract:*")
    text = caplog.text
    for s in ("k1", "k2", "t3"):
        assert s not in text
    assert "key=<REDACTED>" in text
    assert "api_key=<REDACTED>" in text
    assert "token=<REDACTED>" in text
    # Preserved non-sensitive param
    assert "for=tract:*" in text
