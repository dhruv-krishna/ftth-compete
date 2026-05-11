"""FCC Broadband Data Collection (BDC) ingestion and querying.

Public Data API at https://bdc.fcc.gov/api/public/map. Auth is the
header pair {username, hash_value} where hash_value is the raw 44-char
token from the BDC "Manage API Access" page (not actually hashed,
despite the name). See `.claude/data-sources.md`.

Pipeline:
    1. list_as_of_dates() to find latest release.
    2. list_availability_data(as_of) to enumerate files.
    3. download_file(file_id, dest) for each per-state per-tech CSV ZIP.
    4. ingest_state() converts CSVs to a single per-state parquet.
    5. coverage_matrix(geoids) runs a DuckDB query for a tract list.

The Fixed Broadband CSVs include `block_geoid` (15-char 2020 block).
Tract GEOID is the first 11 chars; that's how we aggregate.
"""

from __future__ import annotations

import functools
import logging
import zipfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import duckdb
import httpx
import polars as pl

from ..config import get_settings
from .tiger import STATE_FIPS

log = logging.getLogger(__name__)

API_BASE: Final = "https://bdc.fcc.gov/api/public/map"

# Fixed technology codes used across the pipeline.
TECH_DSL = 10
TECH_CABLE = 40
TECH_FIBER = 50
TECH_GSO_SAT = 60
TECH_NGSO_SAT = 61
TECH_FW_UNLICENSED = 70
TECH_FW_LICENSED = 71
TECH_FW_LBR = 72

FIBER_TECHS = frozenset({TECH_FIBER})
WIRED_TECHS = frozenset({TECH_DSL, TECH_CABLE, TECH_FIBER})
WIRELESS_TECHS = frozenset({TECH_FW_UNLICENSED, TECH_FW_LICENSED, TECH_FW_LBR})
SATELLITE_TECHS = frozenset({TECH_GSO_SAT, TECH_NGSO_SAT})

TECH_LABEL: Final[dict[int, str]] = {
    TECH_DSL: "DSL",
    TECH_CABLE: "Cable",
    TECH_FIBER: "Fiber",
    TECH_GSO_SAT: "GSO Satellite",
    TECH_NGSO_SAT: "Non-GSO Satellite",
    TECH_FW_UNLICENSED: "Unlicensed FW",
    TECH_FW_LICENSED: "Licensed FW",
    TECH_FW_LBR: "Licensed-by-Rule FW",
}

@dataclass(frozen=True)
class BdcFileMeta:
    """One file entry from listAvailabilityData (verified against 2025-06-30 schema).

    Real fields observed:
      file_id, category (Provider/State/Summary), subcategory,
      technology_type (Fixed Broadband / Mobile Broadband),
      technology_code (numeric string, comma-separated for multi-tech providers:
        "50" or "40, 50" or "10, 40, 50, 70" — Comcast files Cable+Fiber as
        "40, 50", Lumen files DSL+Fiber as "10, 50", etc.),
      technology_code_desc, state_fips, provider_id, provider_name,
      file_type (csv/gis), file_name, record_count, speed_tier.
    """

    file_id: str
    file_name: str
    state_fips: str | None
    category: str | None  # Provider / State / Summary
    subcategory: str | None
    technology_type: str | None  # "Fixed Broadband" or "Mobile Broadband"
    technology_codes: tuple[int, ...]  # parsed from comma-separated string
    file_type: str | None  # csv / gis
    provider_id: str | None
    provider_name: str | None
    raw: dict[str, Any]


def _headers() -> dict[str, str]:
    s = get_settings()
    if not s.fcc_username or not s.fcc_api_token:
        raise RuntimeError(
            "FCC_USERNAME and FCC_API_TOKEN must be set in .env. "
            "Register at https://apps.fcc.gov/cores/userLogin.do then generate "
            "an API token at https://broadbandmap.fcc.gov/."
        )
    return {
        "username": s.fcc_username,
        "hash_value": s.fcc_api_token,
        "Accept": "application/json",
    }


def _get(path: str, *, timeout: float = 30.0) -> Any:
    """GET with automatic backoff on 429 Too Many Requests.

    The FCC BDC API rate-limits `/listAsOfDates` to ~10 requests/min.
    Concurrent screener workers blow through that instantly. Retries on
    429 with exponential backoff (1s, 3s, 7s, 15s) so screeners survive
    the rate limit instead of erroring every market after the threshold.
    """
    import time
    url = f"{API_BASE}{path}"
    log.info("BDC GET %s", path)
    backoffs = [1.0, 3.0, 7.0, 15.0]
    last_exc: Exception | None = None
    for attempt, wait in enumerate([0.0, *backoffs]):
        if wait:
            log.warning("BDC %s hit 429; retrying in %.0fs", path, wait)
            time.sleep(wait)
        try:
            r = httpx.get(
                url, headers=_headers(), timeout=timeout, follow_redirects=True,
            )
        except httpx.HTTPError as exc:
            last_exc = exc
            continue
        if r.status_code == 401:
            raise RuntimeError(
                "FCC BDC API returned 401 Unauthorized. Check FCC_USERNAME and "
                "FCC_API_TOKEN in .env. Token is the 44-char string from the BDC "
                "'Manage API Access' page; username is your registered email."
            )
        if r.status_code == 429 and attempt < len(backoffs):
            continue
        r.raise_for_status()
        return r.json()
    if last_exc:
        raise last_exc
    raise RuntimeError(f"BDC GET {path} failed after {len(backoffs) + 1} attempts.")


@functools.lru_cache(maxsize=1)
def _list_as_of_dates_cached() -> tuple[dict[str, Any], ...]:
    body = _get("/listAsOfDates")
    data = body.get("data") if isinstance(body, dict) else body
    return tuple(data) if data else ()


def list_as_of_dates() -> list[dict[str, Any]]:
    """List all BDC release dates. Cached in-process — the FCC's
    `/listAsOfDates` is rate-limited to ~10 req/min, so concurrent
    screener workers would otherwise hammer it into 429s."""
    return list(_list_as_of_dates_cached())


@functools.lru_cache(maxsize=16)
def _list_availability_data_cached(as_of: str) -> tuple[dict[str, Any], ...]:
    body = _get(f"/downloads/listAvailabilityData/{as_of}", timeout=60.0)
    data = body.get("data") if isinstance(body, dict) else body
    return tuple(data) if data else ()


def list_availability_data(as_of: str) -> list[dict[str, Any]]:
    """List all fixed/mobile availability files for a release.

    `as_of` should be in the format the API expects (typically YYYY-MM-DD).
    Cached per release so repeated lookups don't trigger 429s.
    """
    return list(_list_availability_data_cached(as_of))


def previous_release(current: str, *, months_back: int = 12) -> str:
    """Find the published release (with files) closest to ~`months_back` months
    before `current`.

    BDC publishes release dates monthly but only some have actual files
    behind them (biannually). Walking N steps in the sorted list lands on
    placeholder dates; instead we compute a target date by subtracting months
    from `current`, sort candidates by distance to target, and pick the
    closest one whose `listAvailabilityData` is non-empty.

    Defaults to 12 months for year-over-year comparison.
    """
    from datetime import date

    cur_dt = date.fromisoformat(current)
    target_year = cur_dt.year
    target_month = cur_dt.month - months_back
    while target_month < 1:
        target_year -= 1
        target_month += 12
    # Use day=1 to avoid invalid dates like Feb 30; we only care about ranking
    # nearby releases by approximate distance.
    target = date(target_year, target_month, 1)

    dates = list_as_of_dates()
    candidates: list[tuple[int, str]] = []
    for d in dates:
        ds = d.get("as_of_date") or ""
        if not ds or ds >= current:
            continue
        d_dt = date.fromisoformat(ds)
        diff = abs((d_dt - target).days)
        candidates.append((diff, ds))
    candidates.sort()

    for diff, ds in candidates:
        if list_availability_data(ds):
            log.info(
                "previous_release(%s, %d months back) -> %s (%d days from target)",
                current, months_back, ds, diff,
            )
            return ds
    raise RuntimeError(
        f"No earlier release with published files found near {target.isoformat()}"
    )


def trajectory_releases(
    current: str, *, n_points: int = 4, months_step: int = 6
) -> list[str]:
    """Return up to `n_points` published BDC releases stepping back from
    `current` at ~`months_step` intervals, INCLUDING `current` as the most
    recent.

    BDC publishes biannually (~Jun / ~Dec), so the natural step is 6 months.
    Releases without files (placeholder dates) are skipped. Result is sorted
    chronologically ASC (oldest first), suitable for sparklines.
    """
    from datetime import date

    cur_dt = date.fromisoformat(current)
    dates = list_as_of_dates()
    published = sorted({d.get("as_of_date", "") for d in dates if d.get("as_of_date")})

    out: list[str] = [current]
    for step in range(1, n_points):
        target_year = cur_dt.year
        target_month = cur_dt.month - months_step * step
        while target_month < 1:
            target_year -= 1
            target_month += 12
        target = date(target_year, target_month, 1)
        # Find closest published release earlier than current and earlier than
        # any already chosen. Walk candidates in order of distance to target.
        candidates: list[tuple[int, str]] = []
        for ds in published:
            if not ds or ds >= current or ds in out:
                continue
            try:
                d_dt = date.fromisoformat(ds)
            except ValueError:
                continue
            candidates.append((abs((d_dt - target).days), ds))
        candidates.sort()
        for _diff, ds in candidates:
            try:
                if list_availability_data(ds):
                    out.append(ds)
                    break
            except Exception:  # noqa: BLE001
                continue
    out.sort()
    return out


@functools.lru_cache(maxsize=2)
def latest_release(*, with_files: bool = True) -> str:
    """Return the most recent as_of date string.

    If `with_files=True` (default), walks releases newest-first and returns
    the first one whose listAvailabilityData response is non-empty. The most
    recent release dates are sometimes published before their files are,
    so the freshest *usable* release lags by ~1 month.

    Cached in-process — concurrent screener workers all call this and
    would otherwise burn the FCC's 10/min rate budget instantly.
    """
    dates = list_as_of_dates()
    if not dates:
        raise RuntimeError("BDC API returned no as-of dates")

    sorted_dates = sorted(
        (d.get("as_of_date", "") for d in dates if d.get("as_of_date")),
        reverse=True,
    )
    if not sorted_dates:
        raise RuntimeError(f"No as_of_date field in releases: {dates[:3]!r}")

    if not with_files:
        return sorted_dates[0]

    for d in sorted_dates:
        if list_availability_data(d):
            return d
    raise RuntimeError("No release with published files found")


def _parse_tech_codes(raw: Any) -> tuple[int, ...]:
    """Parse the technology_code field which may be a single string code or
    comma-separated list (e.g., "50" or "40, 50" or "10, 40, 50, 70")."""
    if raw is None:
        return ()
    out: list[int] = []
    for token in str(raw).split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.append(int(token))
        except ValueError:
            continue
    return tuple(out)


def _coerce_meta(row: dict[str, Any]) -> BdcFileMeta:
    """Extract standard fields, keeping raw for debugging."""
    return BdcFileMeta(
        file_id=str(row.get("file_id") or ""),
        file_name=str(row.get("file_name") or ""),
        state_fips=str(row["state_fips"]) if row.get("state_fips") else None,
        category=row.get("category"),
        subcategory=row.get("subcategory"),
        technology_type=row.get("technology_type"),
        technology_codes=_parse_tech_codes(row.get("technology_code")),
        file_type=row.get("file_type"),
        provider_id=str(row["provider_id"]) if row.get("provider_id") else None,
        provider_name=row.get("provider_name"),
        raw=row,
    )


def download_file(
    file_id: str,
    dest: Path,
    *,
    data_type: str = "availability",
    file_type_num: int = 1,
    overwrite: bool = False,
) -> Path:
    """Stream-download a BDC file by file_id.

    Endpoint: GET /downloads/downloadFile/{data_type}/{file_id}/{file_type_num}.
    `data_type` corresponds to the upstream listing endpoint
    (`availability` for files from listAvailabilityData). `file_type_num=1`
    selects the CSV-zip variant. Returns 405 "Method Not Available" if any
    path segment is missing or the headers are unauthenticated.
    """
    if dest.exists() and not overwrite:
        # Validate the cached file is a real zip — corrupt/partial downloads
        # from a previous run will otherwise blow up downstream forever.
        # `is_zipfile` is cheap (reads the EOCD record only).
        if zipfile.is_zipfile(dest):
            log.info("BDC file already at %s, skipping", dest)
            return dest
        log.warning(
            "Cached BDC file %s is not a valid zip (likely a partial or "
            "errored download). Removing and re-fetching.",
            dest,
        )
        try:
            dest.unlink()
        except OSError:
            pass
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{API_BASE}/downloads/downloadFile/{data_type}/{file_id}/{file_type_num}"
    log.info("BDC download %s -> %s", file_id, dest)
    with httpx.stream(
        "GET", url, headers=_headers(), timeout=600.0, follow_redirects=True
    ) as r:
        if r.status_code == 401:
            raise RuntimeError("FCC BDC API returned 401 - check credentials")
        if r.status_code == 405:
            raise RuntimeError(
                f"BDC downloadFile returned 405. URL: {url}. "
                "Verify data_type / file_type_num path segments."
            )
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
    # Post-download sanity check. If the server returned HTML/JSON instead
    # of a zip (auth issue, file not yet published, etc.) we'd otherwise
    # cache the garbage and re-hit the BadZipFile cliff on every run.
    if not zipfile.is_zipfile(dest):
        try:
            dest.unlink()
        except OSError:
            pass
        raise RuntimeError(
            f"BDC downloadFile returned non-zip content for file_id={file_id}. "
            f"URL: {url}. File deleted; re-run to retry."
        )
    return dest


def _bdc_dir(as_of: str) -> Path:
    return get_settings().raw_dir / "bdc" / as_of


def _processed_path(as_of: str, state_fips: str) -> Path:
    return get_settings().processed_dir / "bdc" / as_of / f"state={state_fips}.parquet"


@contextmanager
def _open_csv_in_zip(zip_path: Path) -> Iterator[Path]:
    """Extract the first .csv from a ZIP to a sibling temp dir; yield the CSV path.

    If the file isn't actually a zip (corrupt cache from an aborted prior
    download, server returning HTML/JSON in error), delete it so the next
    run re-downloads cleanly rather than crashing on the same bad bytes.
    """
    extract_dir = zip_path.with_suffix("")
    extract_dir.mkdir(parents=True, exist_ok=True)
    try:
        zf = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile as exc:
        try:
            zip_path.unlink()
        except OSError:
            pass
        raise RuntimeError(
            f"Corrupt BDC zip at {zip_path} deleted; re-run to refetch."
        ) from exc
    with zf:
        csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_members:
            raise RuntimeError(f"No CSV in {zip_path}")
        zf.extract(csv_members[0], extract_dir)
        yield extract_dir / csv_members[0]


_FIXED_TECH_CODES: Final = frozenset(
    {
        TECH_DSL, TECH_CABLE, TECH_FIBER,
        TECH_GSO_SAT, TECH_NGSO_SAT,
        TECH_FW_UNLICENSED, TECH_FW_LICENSED, TECH_FW_LBR,
    }
)


def _is_fixed_for_state(meta: BdcFileMeta, fips: str) -> bool:
    """Filter listAvailabilityData rows to per-provider fixed-broadband CSVs for a state.

    Match criteria (per FCC BDC API schema observed 2025-06-30):
      - state_fips matches
      - category == "Provider"  (per-provider rows; we skip "State" and "Summary"
        rollups since we want full provider granularity)
      - technology_type == "Fixed Broadband" (excludes mobile)
      - file_type == "csv" (excludes "gis" shapefile/geopackage)
      - any of the technology_codes overlaps the 8 fixed tech codes
        (multi-tech providers like Comcast file as "40, 50" — Cable + Fiber)
    """
    if meta.state_fips != fips:
        return False
    if (meta.category or "").lower() != "provider":
        return False
    if (meta.technology_type or "").lower() != "fixed broadband":
        return False
    if (meta.file_type or "").lower() != "csv":
        return False
    return any(code in _FIXED_TECH_CODES for code in meta.technology_codes)


def ingest_state(state: str, as_of: str | None = None) -> Path:
    """Download all fixed-broadband CSVs for a state and collapse into one parquet.

    Returns the path to the resulting parquet file. Idempotent: if the
    parquet already exists it's returned without re-downloading.
    """
    fips = STATE_FIPS[state.upper()]
    if as_of is None:
        as_of = latest_release()

    parquet_path = _processed_path(as_of, fips)
    if parquet_path.exists():
        log.info("BDC parquet for state %s release %s already at %s", fips, as_of, parquet_path)
        return parquet_path

    raw_root = _bdc_dir(as_of)
    raw_root.mkdir(parents=True, exist_ok=True)

    files = list_availability_data(as_of)
    metas = [_coerce_meta(row) for row in files]
    state_metas = [m for m in metas if _is_fixed_for_state(m, fips)]
    if not state_metas:
        # Fall back: include any state-matching CSV; user can inspect raw.
        log.warning(
            "Strict filter found no files for state=%s in %s. "
            "Surfacing raw API response for inspection.",
            fips, as_of,
        )
        for m in metas[:5]:
            log.warning("Sample: %s", m.raw)
        raise RuntimeError(
            f"No fixed-broadband files found for state {state} ({fips}) in {as_of}. "
            "Inspect logs for raw API response shape; field names may have shifted."
        )

    csv_paths: list[Path] = []
    for m in state_metas:
        tech_tag = "_".join(str(c) for c in m.technology_codes) or "x"
        zip_dest = raw_root / f"{fips}_tech{tech_tag}_p{m.provider_id}_{m.file_id}.zip"
        download_file(m.file_id, zip_dest)
        with _open_csv_in_zip(zip_dest) as csv_path:
            csv_paths.append(csv_path)

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    _csvs_to_parquet(csv_paths, parquet_path)
    log.info("BDC ingest done: %d CSVs -> %s", len(csv_paths), parquet_path)
    return parquet_path


def _csvs_to_parquet(csv_paths: Iterable[Path], out: Path) -> None:
    """Combine BDC CSVs into a single parquet partitioned by state.

    Uses Polars lazy reads + scan_csv with schema inference. Adds a
    derived `tract_geoid` column (first 11 chars of block_geoid).
    """
    frames: list[pl.LazyFrame] = []
    for p in csv_paths:
        # BDC CSVs use block_geoid as 15-digit string; force string type to
        # preserve leading zeros.
        lf = pl.scan_csv(
            p,
            schema_overrides={"block_geoid": pl.Utf8, "h3_res8_id": pl.Utf8},
            ignore_errors=True,
        )
        frames.append(lf)
    combined = pl.concat(frames, how="diagonal_relaxed")
    combined = combined.with_columns(
        pl.col("block_geoid").str.slice(0, 11).alias("tract_geoid"),
    )
    combined.sink_parquet(out)


def location_availability(
    geoids: list[str], *, as_of: str | None = None
) -> pl.DataFrame:
    """Per-tract location-level technology availability.

    Returns a frame with one row per tract:
        tract_geoid, total_locations, fiber_locations, cable_locations,
        dsl_locations, fw_locations, sat_locations.

    A "location" is a unique FCC BSL (`location_id`). The counts represent
    distinct locations where AT LEAST ONE provider offers that technology
    — the household-availability question, not the provider-count question.

    Critical for the Overview "Fiber available %" metric: a market with 5
    fiber providers and 7 non-fiber providers might still have ~80% fiber
    availability at the household level if the fiber providers' footprints
    union to most addresses.
    """
    if not geoids:
        return pl.DataFrame()

    state_fips_set = {g[:2] for g in geoids}
    if as_of is None:
        as_of = latest_release()

    parquet_paths: list[Path] = []
    for fips in state_fips_set:
        state_abbr = next((s for s, f in STATE_FIPS.items() if f == fips), None)
        if state_abbr is None:
            log.warning("No state abbreviation for FIPS %s; skipping", fips)
            continue
        parquet_paths.append(ingest_state(state_abbr, as_of))

    if not parquet_paths:
        return pl.DataFrame()

    geoid_list = ", ".join(f"'{g}'" for g in geoids)
    parquet_glob = ", ".join(f"'{p.as_posix()}'" for p in parquet_paths)

    sql = f"""
    SELECT
        tract_geoid,
        COUNT(DISTINCT location_id) AS total_locations,
        COUNT(DISTINCT CASE WHEN technology = {TECH_FIBER} THEN location_id END) AS fiber_locations,
        COUNT(DISTINCT CASE WHEN technology = {TECH_CABLE} THEN location_id END) AS cable_locations,
        COUNT(DISTINCT CASE WHEN technology = {TECH_DSL} THEN location_id END) AS dsl_locations,
        COUNT(DISTINCT CASE WHEN technology IN ({TECH_FW_UNLICENSED}, {TECH_FW_LICENSED}, {TECH_FW_LBR}) THEN location_id END) AS fw_locations,
        COUNT(DISTINCT CASE WHEN technology IN ({TECH_GSO_SAT}, {TECH_NGSO_SAT}) THEN location_id END) AS sat_locations
    FROM read_parquet([{parquet_glob}])
    WHERE tract_geoid IN ({geoid_list})
    GROUP BY tract_geoid
    ORDER BY tract_geoid
    """

    con = duckdb.connect(":memory:")
    try:
        result = con.execute(sql).pl()
    finally:
        con.close()
    return result


def coverage_matrix(geoids: list[str], *, as_of: str | None = None) -> pl.DataFrame:
    """Return a tract x provider x technology coverage frame for the given tracts.

    Group keys: tract_geoid, provider_id, brand_name, technology.
    Aggregates: location count, max advertised down/up speed.
    """
    if not geoids:
        return pl.DataFrame()

    # Determine which state(s) we need
    state_fips_set = {g[:2] for g in geoids}
    if as_of is None:
        as_of = latest_release()

    parquet_paths: list[Path] = []
    for fips in state_fips_set:
        # Find the state abbreviation reverse-mapped from FIPS
        state_abbr = next((s for s, f in STATE_FIPS.items() if f == fips), None)
        if state_abbr is None:
            log.warning("No state abbreviation for FIPS %s; skipping", fips)
            continue
        parquet_paths.append(ingest_state(state_abbr, as_of))

    if not parquet_paths:
        return pl.DataFrame()

    geoid_list = ", ".join(f"'{g}'" for g in geoids)
    parquet_glob = ", ".join(f"'{p.as_posix()}'" for p in parquet_paths)

    sql = f"""
    SELECT
        tract_geoid,
        provider_id,
        brand_name,
        technology,
        COUNT(DISTINCT location_id) AS locations_served,
        COUNT(DISTINCT CASE WHEN max_advertised_download_speed >= 1000 THEN location_id END) AS gig_locations,
        COUNT(DISTINCT CASE WHEN max_advertised_download_speed >= 100 AND max_advertised_download_speed < 1000 THEN location_id END) AS hundred_locations,
        COUNT(DISTINCT CASE WHEN max_advertised_download_speed < 100 THEN location_id END) AS sub_hundred_locations,
        MAX(max_advertised_download_speed) AS max_down,
        MAX(max_advertised_upload_speed)   AS max_up,
        BOOL_OR(low_latency = 1) AS any_low_latency
    FROM read_parquet([{parquet_glob}])
    WHERE tract_geoid IN ({geoid_list})
    GROUP BY tract_geoid, provider_id, brand_name, technology
    ORDER BY tract_geoid, provider_id, technology
    """

    con = duckdb.connect(":memory:")
    try:
        result = con.execute(sql).pl()
    finally:
        con.close()
    return result
