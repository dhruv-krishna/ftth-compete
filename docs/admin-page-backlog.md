# Admin sidecar — backlog

Ideas for what to put on `/admin` beyond the current event-log table. None of these are urgent; capture so we don't lose them.

## Blocker: persistence

The `visitors.db` SQLite file lives on HF Spaces free-tier ephemeral disk and is wiped on every container restart (every deploy + every long-idle sleep). Any panel that requires "more than the current container's lifetime" of data is misleading without a persistence layer underneath.

Three ways to fix this; **do this before building most of the panels below.**

| Option | Cost | Effort | Survives all restarts? |
|---|---|---|---|
| **A. HF persistent storage** | $5/mo | 5 min config | Yes, automatically; also generalises to BDC cache, Places cache, etc. |
| **B. Backup `visitors.db` to a private HF Dataset** (same pattern as the public BDC cache) | Free | ~45 min code | Yes, with up to 5 min lag |
| **C. Stream events to an external service** (Plausible, GoatCounter, Sentry free tier) | Free for low volume | ~30 min | Yes, infinite retention, but lives on a separate dashboard not in-app |

Recommended: **B** — same playbook as `scripts/refresh_bdc_states.py` + the Dockerfile `curl` block, but writes back periodically. Boot restores from dataset; every 5 min an asyncio task gzips + uploads. Private dataset because IP hashes + UA strings are PII-adjacent.

Stop-gap if not doing any of these yet: prefix every panel with "since `<container_boot_time>`" so it's honest about scope.

## Panels — who's using it

1. **Sessions table.** Group all events under each Reflex session token. Columns: started-at, duration, # events, markets viewed, last action. Click row → that session's event timeline. The single biggest analytic unlock vs the current flat event log.
2. **Per-day sessions/events chart.** Last 30 days as a small SVG bar chart at the top of the page.
3. **Hour-of-day heatmap.** 24×7 grid showing when people actually use it (UTC + local TZ).
4. **Returning vs new.** Track if a session_id / IP hash was seen on a previous day. "12 new, 3 returning" in the summary line.
5. **Geographic distribution.** Bake MaxMind GeoLite2 country MMDB into the image (~50 MB, free), resolve hashed IPs to country only (not city — keep PII surface minimal).

## Panels — what they're doing

6. **Top markets this week / all time.** Most-searched cities, ranked. The most useful single insight for "what should I make sure performs well".
7. **Top providers clicked.** Which providers people drill into most.
8. **Lens distribution.** % defensive / offensive / neutral. Tells you which lens is actually used vs decoration.
9. **Search funnel.** % of sessions hitting `market_lookup` → `tract_click` → `provider_click`. Where do people drop off.
10. **Markets that resolved to errors.** Surface city names that triggered `lookup_error` — catches typos and underserved cities.

## Panels — system health

11. **Phase timing histogram.** Record A1/A2/B1/B2 wall-times per lookup; render p50/p95/max bars. Catches performance regressions.
12. **Slow lookups list.** Last 20 lookups sorted by duration descending. Quick spot for "which market made this person wait 90s today".
13. **Cache hit rates.** LRU on `run_market` (hit/miss counters via `_run_market_cached.cache_info()`), disk parquet hits. Tells you if the LRU is helping or just wasting RAM.
14. **Disk usage.** Total `FTTH_DATA_DIR` size + breakdown by subdir (`raw/bdc/`, `raw/ias/`, `raw/tiger/`, `processed/`, `visitors.db`). HF free tier disk is finite.
15. **Container uptime.** Seconds since boot + last restart timestamp. Helps spot HF restarts that reset caches.
16. **API quota usage.** Google Places: today's count toward the 1K/mo free tier. Pull from Places response when available, else estimate from event log.
17. **Data freshness.** Table of every data source with "current release / last refreshed / age". BDC `2025-06-30`, ACS `2020-2024`, IAS, TIGER, etc. From `data_versions` on a representative TearSheet.

## Panels — operations

18. **Clear LRU cache.** POST button → `_run_market_cached.cache_clear()`. Useful after pushing code changes when you want fresh lookups.
19. **Wipe visitor log.** POST button with confirm step, deletes all rows from `events`. Privacy compliance + cleaning up after testing.
20. **Export events as CSV.** Download button generating `events.csv` with one row per event. Lets you do real analysis offline.
21. **Pre-warm states.** Text input + button that backgrounds `fcc_bdc.coverage_matrix(['some_geoid'])` for given states. Warm cache before a planned demo.

## Panels — meta / build info

22. **Build version footer.** Git commit SHA + image build timestamp at the bottom of the admin page. Resolves "am I looking at the latest code?"
23. **Recently deployed commits.** Last 5 commits from `git log` baked into the image.
24. **Available cached data.** Which states have BDC parquets, which IAS releases are seeded, which TIGER state files exist. Spot gaps.

## Panels — privacy / security

25. **Failed admin access log.** Capture every 404 to `/admin` with the attempted key (truncated/hashed). Spot brute-force attempts.
26. **Bot vs human heuristics.** Flag sessions whose UA is `curl`, `python-httpx`, `bot`, etc. Useful for filtering out cron-job.org keepalive pings + any monitoring.

## Recommended top 5 (post-persistence)

If/when we do persistence first, the tightest first batch (~2 hrs total):

1. Sessions table (#1) — biggest analytic unlock; turns flat events into visitor stories.
2. Top markets this week (#6) — the question you'll ask most.
3. Per-day sessions chart (#2) — trend at a glance.
4. Phase timing histogram (#11) — performance regression detection.
5. Export as CSV (#20) — escape hatch when the UI can't answer something.
