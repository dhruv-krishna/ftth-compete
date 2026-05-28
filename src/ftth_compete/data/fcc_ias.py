"""FCC Internet Access Services (IAS) tract-level subscription data.

Source: https://www.fcc.gov/form-477-census-tract-data-internet-access-services

What this gets us
-----------------
- Per-tract residential fixed-broadband subscription DENSITY (connections per
  1,000 households), bucketed into 6 codes (0-5).
- Two thresholds: `pcat_all` (any speed >=200 kbps) and `pcat_25x0` (>=25/3
  Mbps as of recent releases).
- All-technology aggregate; **NO per-provider, per-tech, or raw-count
  breakdown** at tract level (provider-specific counts are filed
  confidentially with the FCC).

The FCC's bucket scheme (per the IAS report appendix and the README in each
release ZIP) maps codes to "connections per 1,000 HH" ranges:

    0 = zero connections
    1 = 0-200 / 1000 HH       (low / 0-20% take)
    2 = 200-400               (20-40%)
    3 = 400-600               (40-60%)
    4 = 600-800               (60-80%)
    5 = 800+                  (80%+ — saturated)

We use bucket midpoints to estimate market-level totals, propagating bucket
bounds as low/high uncertainty.

Release cadence and access
--------------------------
Releases through **Jun 2022** are direct ZIPs on `fcc.gov`. Newer releases
(Dec 2022, Jun 2023, Dec 2023, Jun 2024, Dec 2024) are hosted on Box at
`us-fcc.box.com/v/Res-Fixed-Tract-{Mon}-{YYYY}` and require manual download
of a CSV that the user then drops into `data/raw/ias/`. We auto-discover the
newest cached file at lookup time.

For most analyses Jun 2022 is several years stale; users who need fresher
data should grab the latest Box download manually. The pipeline picks up
whichever file is newest in `data/raw/ias/`.
"""

from __future__ import annotations

import io
import logging
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import httpx
import polars as pl

from ..config import get_settings

log = logging.getLogger(__name__)

# Direct-download ZIP URLs. FCC consistently named these `tract_map_{mon}_{yyyy}.zip`
# from ~2014 onward; we extrapolate the pattern for older releases. Any URL
# that 404s is silently skipped at fetch time (caller logs which years we got).
# Newest releases (Dec 2022+) are Box-hosted and require manual drop into
# `data/raw/ias/`.
_FCC_BASE: Final[str] = "https://www.fcc.gov/sites/default/files"

def _url_for(as_of: str) -> str:
    """Build the conventional tract_map URL from a YYYY-MM-DD as-of date."""
    yyyy, mm, _ = as_of.split("-")
    mon = "jun" if mm == "06" else "dec"
    return f"{_FCC_BASE}/tract_map_{mon}_{yyyy}.zip"

# Releases known to be hosted at the conventional URL. The Phase 6e historical
# ingest walks this list newest-to-oldest and skips any that 404. Listing them
# explicitly (rather than generating on the fly) makes the cadence visible and
# documents what we expect to find.
DIRECT_RELEASE_URLS: Final[dict[str, str]] = {
    as_of: _url_for(as_of)
    for as_of in (
        "2022-06-30", "2021-12-31", "2021-06-30",
        "2020-12-31", "2020-06-30",
        "2019-12-31", "2019-06-30",
        "2018-12-31", "2018-06-30",
        "2017-12-31", "2017-06-30",
        "2016-12-31", "2016-06-30",
        "2015-12-31", "2015-06-30",
        "2014-12-31", "2014-06-30",
    )
}

# Bucket -> (low, mid, high) connections per 1,000 HH.
# 1000 cap for code 5 is conservative; some tracts genuinely exceed
# 1000 (multi-line households) but FCC caps the published code there.
_BUCKETS: Final[dict[int, tuple[float, float, float]]] = {
    0: (0.0, 0.0, 0.0),
    1: (1.0, 100.0, 200.0),
    2: (200.0, 300.0, 400.0),
    3: (400.0, 500.0, 600.0),
    4: (600.0, 700.0, 800.0),
    5: (800.0, 900.0, 1000.0),
}


@dataclass(frozen=True)
class IasRelease:
    """A loaded IAS release."""

    as_of: str  # e.g., "2022-06-30"
    source: str  # local path or remote URL
    frame: pl.DataFrame  # tract_geoid, bucket_all, bucket_25x0


def _ias_dir() -> Path:
    return get_settings().raw_dir / "ias"


def _list_local_zips() -> list[tuple[str, Path]]:
    """List cached IAS ZIPs as (as_of_date, path) sorted newest-first."""
    d = _ias_dir()
    if not d.exists():
        return []
    out: list[tuple[str, Path]] = []
    for p in d.glob("tract_map_*.zip"):
        # Extract as-of from filename like tract_map_dec_2024.zip
        m = re.match(r"tract_map_(jun|dec|june|december)_?(\d{4})", p.stem.lower())
        if not m:
            continue
        period = m.group(1)
        year = int(m.group(2))
        if period.startswith("jun"):
            as_of = f"{year:04d}-06-30"
        else:
            as_of = f"{year:04d}-12-31"
        out.append((as_of, p))
    out.sort(key=lambda kv: kv[0], reverse=True)
    return out


def _download_zip(as_of: str, *, strict: bool = True) -> Path | None:
    """Download a direct-URL release.

    Strict mode (default) raises if the URL isn't in the registry or 404s.
    `strict=False` returns None instead — used by `historical_releases()`
    which sweeps every known year and tolerates gaps.
    """
    url = DIRECT_RELEASE_URLS.get(as_of)
    if not url:
        if strict:
            raise RuntimeError(
                f"No direct-download URL for IAS release {as_of}. "
                "Newer releases are Box-hosted at us-fcc.box.com — download "
                f"manually and place the .zip in {_ias_dir()}."
            )
        return None
    dest_dir = _ias_dir()
    dest_dir.mkdir(parents=True, exist_ok=True)
    fname = url.rsplit("/", 1)[-1]
    dest = dest_dir / fname
    if dest.exists():
        # Validate cached file is a real zip — corrupt/partial downloads
        # from a prior aborted run otherwise crash downstream forever.
        if zipfile.is_zipfile(dest):
            log.debug("IAS zip already cached: %s", dest)
            return dest
        log.warning("Cached IAS zip %s is corrupt; removing and refetching.", dest)
        try:
            dest.unlink()
        except OSError:
            pass
    log.info("Downloading IAS %s from %s", as_of, url)
    try:
        with httpx.stream(
            "GET", url, timeout=120.0, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ftth-compete/0.1)"},
        ) as r:
            if r.status_code == 404:
                if strict:
                    r.raise_for_status()
                log.info("IAS release %s not at %s (404) — skipping.", as_of, url)
                return None
            r.raise_for_status()
            with dest.open("wb") as f:
                for chunk in r.iter_bytes(chunk_size=1 << 20):
                    f.write(chunk)
    except httpx.HTTPError as exc:
        if dest.exists():
            try:
                dest.unlink()
            except OSError:
                pass
        if strict:
            raise
        log.warning("IAS %s download failed (%s) — skipping.", as_of, exc)
        return None
    if not zipfile.is_zipfile(dest):
        try:
            dest.unlink()
        except OSError:
            pass
        if strict:
            raise RuntimeError(f"IAS download for {as_of} is not a valid zip.")
        return None
    return dest


def _read_csv_from_zip(zip_path: Path) -> pl.DataFrame:
    """Read the tract_map CSV out of an IAS ZIP and normalize columns."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV inside {zip_path}")
        with zf.open(csv_names[0]) as f:
            buf = io.BytesIO(f.read())

    # FCC CSVs use header row tractcode,pcat_all,pcat_25x0 (or 25x3 historically)
    df = pl.read_csv(buf, schema_overrides={"tractcode": pl.Utf8})

    # Normalize column names
    rename_map: dict[str, str] = {"tractcode": "tract_geoid"}
    for col in df.columns:
        low = col.lower()
        if low == "pcat_all":
            rename_map[col] = "bucket_all"
        elif low in {"pcat_25x3", "pcat_25x0", "pcat_25"}:
            rename_map[col] = "bucket_25"
    df = df.rename(rename_map)

    # Ensure bucket columns are int
    for col in ("bucket_all", "bucket_25"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int8, strict=False))

    return df


def latest_release(*, auto_download: bool = True) -> IasRelease:
    """Return the newest available IAS release.

    Picks the newest local cached ZIP first; otherwise downloads Jun 2022 (the
    newest direct-download release). Newer Box-hosted releases require a
    manual user step (see module docstring).
    """
    local = _list_local_zips()
    if local:
        as_of, path = local[0]
        log.info("Using cached IAS release: %s (%s)", as_of, path)
        return IasRelease(
            as_of=as_of, source=str(path), frame=_read_csv_from_zip(path)
        )

    if not auto_download:
        raise RuntimeError(
            "No IAS data cached and auto_download=False. "
            f"Drop a tract_map_*.zip into {_ias_dir()} or call with auto_download=True."
        )

    # Default: download the newest direct-URL release (Jun 2022)
    as_of = max(DIRECT_RELEASE_URLS.keys())
    path = _download_zip(as_of)
    log.warning(
        "Using IAS release %s (latest free direct-download). "
        "For fresher data, manually download from "
        "https://www.fcc.gov/form-477-census-tract-data-internet-access-services "
        "(Box links in the 'Recent releases' list) and drop the .zip into %s.",
        as_of, _ias_dir(),
    )
    return IasRelease(as_of=as_of, source=str(path), frame=_read_csv_from_zip(path))


def load_tract_subs(geoids: list[str], *, auto_download: bool = True) -> IasRelease:
    """Return an IasRelease whose `frame` is filtered to the given tract GEOIDs."""
    rel = latest_release(auto_download=auto_download)
    if not geoids:
        return IasRelease(as_of=rel.as_of, source=rel.source, frame=rel.frame.head(0))
    filtered = rel.frame.filter(pl.col("tract_geoid").is_in(geoids))
    return IasRelease(as_of=rel.as_of, source=rel.source, frame=filtered)


def bucket_midpoint(code: int | None) -> tuple[float, float, float]:
    """Return (low, mid, high) connections per 1000 HH for a bucket code.

    Returns (0, 0, 0) for unknown / null codes — conservative default.
    """
    if code is None:
        return (0.0, 0.0, 0.0)
    return _BUCKETS.get(int(code), (0.0, 0.0, 0.0))


# ---------------------------------------------------------------------------
# Phase 6e: Historical IAS time series (2014→present).
#
# Sweeps every known direct-URL release, fills in any Box-hosted releases the
# user has manually dropped into data/raw/ias/, and produces a per-market
# subscription-density trajectory. NOT per-provider — public 477 microdata
# doesn't expose provider IDs. This is market-total take-rate evolution only.

def historical_releases(
    *,
    auto_download: bool = True,
    since: str | None = None,
) -> list[IasRelease]:
    """Return all available IAS releases, newest-first.

    Combines:
      1. Locally cached ZIPs under data/raw/ias/ (covers Box-hosted releases
         the user manually downloaded).
      2. Every direct-URL release in DIRECT_RELEASE_URLS that fetches OK
         (tolerates 404s for years the FCC removed or never published at the
         conventional URL).

    `since` is an optional inclusive YYYY-MM-DD lower bound — useful for
    keeping the sweep short during UI lookups.
    """
    seen: set[str] = set()
    out: list[IasRelease] = []

    # 1. Local cache first — covers manually-dropped Box files (Dec 2022+).
    for as_of, path in _list_local_zips():
        if since and as_of < since:
            continue
        if as_of in seen:
            continue
        try:
            frame = _read_csv_from_zip(path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to read cached IAS %s (%s) — skipping.", path, exc)
            continue
        out.append(IasRelease(as_of=as_of, source=str(path), frame=frame))
        seen.add(as_of)

    # 2. Direct-URL releases — download (or use cache) for everything we
    # haven't already loaded from the local sweep above.
    for as_of in sorted(DIRECT_RELEASE_URLS.keys(), reverse=True):
        if as_of in seen:
            continue
        if since and as_of < since:
            continue
        if not auto_download:
            continue
        path = _download_zip(as_of, strict=False)
        if path is None:
            continue
        try:
            frame = _read_csv_from_zip(path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to read IAS %s (%s) — skipping.", path, exc)
            continue
        out.append(IasRelease(as_of=as_of, source=str(path), frame=frame))
        seen.add(as_of)

    out.sort(key=lambda r: r.as_of, reverse=True)
    return out


@dataclass(frozen=True)
class MarketSubscriptionPoint:
    """One time-point in the market-level take-rate trajectory.

    `density_*_per_1k` are connections per 1,000 households averaged across
    the market's tracts. The `take_rate_*` fields divide by 1000 to give a
    fraction (0.0-1.0+) — slight overcount possible because households with
    multiple connections show up once per connection in FCC's count.
    """

    as_of: str
    n_tracts: int
    density_all_per_1k_low: float
    density_all_per_1k_mid: float
    density_all_per_1k_high: float
    density_25_per_1k_low: float
    density_25_per_1k_mid: float
    density_25_per_1k_high: float
    take_rate_all_mid: float       # fraction (mid estimate, any-speed)
    take_rate_25_mid: float        # fraction (mid estimate, ≥25/3 Mbps)


def _tract_mean_density(
    frame: pl.DataFrame, geoids: list[str], col: str,
) -> tuple[float, float, float]:
    """Mean (low, mid, high) per-1k density across geoids for a bucket column.

    Tracts not present in this release (e.g., GEOID was new after 2020 census
    re-tracting, or the tract was suppressed for low-population) contribute
    (0, 0, 0) — conservative.
    """
    if col not in frame.columns:
        return (0.0, 0.0, 0.0)
    sub = frame.filter(pl.col("tract_geoid").is_in(geoids))
    if sub.is_empty() or not geoids:
        return (0.0, 0.0, 0.0)
    lows: list[float] = []
    mids: list[float] = []
    highs: list[float] = []
    bucket_by_geoid = dict(zip(sub["tract_geoid"].to_list(), sub[col].to_list()))
    for g in geoids:
        lo, mi, hi = bucket_midpoint(bucket_by_geoid.get(g))
        lows.append(lo)
        mids.append(mi)
        highs.append(hi)
    n = float(len(geoids))
    return (sum(lows) / n, sum(mids) / n, sum(highs) / n)


def market_subscription_history(
    geoids: list[str],
    *,
    auto_download: bool = True,
    since: str | None = "2015-01-01",
) -> list[MarketSubscriptionPoint]:
    """Per-market subscription-density trajectory across every available IAS release.

    Returns a list of `MarketSubscriptionPoint`, **newest first**. Mean
    densities are computed across the supplied tract GEOIDs; missing tracts
    in older releases (pre-2020 re-tracting) contribute zero. Caller decides
    how to render — typically a sparkline of `take_rate_25_mid` over `as_of`.

    `since` defaults to 2015-01-01 to keep cold-load cost reasonable (drops
    the 2014 sweep which is rarely interesting + sometimes 404s).
    """
    releases = historical_releases(auto_download=auto_download, since=since)
    out: list[MarketSubscriptionPoint] = []
    for rel in releases:
        low_a, mid_a, high_a = _tract_mean_density(rel.frame, geoids, "bucket_all")
        low_25, mid_25, high_25 = _tract_mean_density(rel.frame, geoids, "bucket_25")
        out.append(MarketSubscriptionPoint(
            as_of=rel.as_of,
            n_tracts=len(geoids),
            density_all_per_1k_low=low_a,
            density_all_per_1k_mid=mid_a,
            density_all_per_1k_high=high_a,
            density_25_per_1k_low=low_25,
            density_25_per_1k_mid=mid_25,
            density_25_per_1k_high=high_25,
            take_rate_all_mid=mid_a / 1000.0,
            take_rate_25_mid=mid_25 / 1000.0,
        ))
    return out
