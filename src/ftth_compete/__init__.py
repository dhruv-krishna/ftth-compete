"""ftth-compete — FTTH market competitive intelligence tool."""

from __future__ import annotations

import logging as _logging
import re as _re

__version__ = "0.1.0"

# Use the OS trust store for SSL verification. Required behind corporate
# proxies (e.g. AlticeUSA's SSL inspection) where Python's certifi bundle
# lacks the corporate CA but the OS store has it. Idempotent and silent if
# truststore isn't available.
try:
    import truststore as _truststore

    _truststore.inject_into_ssl()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Logging hygiene: redact secrets from log records.
#
# httpx logs requests at INFO with the FULL URL including query params. We
# pass keys via query string for the Census API (the API only accepts that
# form), so plaintext keys leak into the terminal / log files unless we
# filter. We install a LogRecordFactory that scrubs sensitive query params
# from every record at creation, before any handler sees it — including
# pytest's caplog and any third-party handlers that bypass logger filters.

_SENSITIVE_QUERY_PARAMS = (
    "key",          # Census API key
    "api_key",      # generic
    "apikey",
    "hash_value",   # FCC BDC API token
    "token",
    "access_token",
)

_QUERY_PARAM_RE = _re.compile(
    r"(?P<param>(?:" + "|".join(_SENSITIVE_QUERY_PARAMS) + r"))=[^&\s\"'<>]+",
    flags=_re.IGNORECASE,
)


def _install_log_redactor() -> None:
    """Wrap the LogRecordFactory to scrub sensitive query params (idempotent)."""
    flag = "_ftth_compete_redactor_installed"
    base = _logging.getLogRecordFactory()
    if getattr(base, flag, False):
        return  # already installed; double-import safe

    def _factory(*args: object, **kwargs: object) -> _logging.LogRecord:
        record = base(*args, **kwargs)
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001
            return record
        if "=" not in msg:
            return record
        redacted = _QUERY_PARAM_RE.sub(r"\g<param>=<REDACTED>", msg)
        if redacted != msg:
            record.msg = redacted
            record.args = None  # message already substituted into msg
        return record

    setattr(_factory, flag, True)
    _logging.setLogRecordFactory(_factory)


_install_log_redactor()
