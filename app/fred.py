"""FRED job type: macroeconomic time series from the Federal Reserve (St. Louis).

This plugs FRED into the same plan/run task model the rest of the manager uses
(see :mod:`app.jobs`). One job type, ``fred``, with one resumable task per
series. What makes it "sophisticated" rather than a plain CSV grab:

* **Discovery** — a job needn't list series ids by hand. The planner can expand
  a *release* (every series the BLS employment release publishes), a *category*
  (the whole "Interest Rates" node), or a free-text *search* into their member
  series, dedup the lot against any explicit ids, and fan out one task each.
* **Transformations** — observations can be pulled in any of FRED's built-in
  units (levels, change, % change, year-over-year, log, …), re-aggregated to a
  coarser frequency (daily → monthly average/sum/eop), and bounded to a date
  window — all server-side, so a backtest reads the exact transform it wants.
* **Vintages (ALFRED)** — a real-time window (``realtime_start``/``realtime_end``)
  or explicit ``vintage_dates`` pulls *point-in-time* data as it was first
  published, the right way to avoid look-ahead bias from later revisions.
* **Metadata** — every series also gets a one-row ``_meta.csv`` (title, native
  units/frequency, seasonal adjustment, span, last update, notes).

FRED differs from the key-less sources in two ways handled centrally here: a
mandatory API key (free; ``FRED_API_KEY``) injected on every request, and a
120 req/min ceiling that all worker threads share via :data:`_LIMITER`.
"""
import csv
import os
import threading
import time

import requests

from . import config
from .jobs import RetryableError

FRED_BASE = "https://api.stlouisfed.org/fred"

# FRED's observation unit transformations and frequency-aggregation options,
# validated up front so a typo fails at submit time instead of silently pulling
# untransformed levels. (https://fred.stlouisfed.org/docs/api/fred/)
UNITS = {"lin", "chg", "ch1", "pch", "pc1", "pca", "cch", "cca", "log"}
FREQUENCIES = {"d", "w", "bw", "m", "q", "sa", "a",
               "wef", "weth", "wew", "wetu", "wem", "wesu", "wesa", "bwew", "bwem"}
AGG_METHODS = {"avg", "sum", "eop"}

# Safety cap on how many series a single discovery (release/category/search) is
# allowed to fan out into, so a broad query can't queue tens of thousands of
# tasks by accident. Override per-job with params["max_series"].
DEFAULT_MAX_SERIES = 1000


# ---------------------------------------------------------------------------
# Shared rate limiter — every FRED request goes through this so the whole
# process stays under FRED's 120 req/min ceiling no matter how many worker
# threads are downloading at once. (Same design as app.sec._RateLimiter.)
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Hands each caller a monotonically spaced time slot, then sleeps until it."""

    def __init__(self, rate_per_sec: float):
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._lock = threading.Lock()
        self._next = 0.0

    def acquire(self):
        if self._interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next)
            self._next = slot + self._interval
        wait = slot - time.monotonic()
        if wait > 0:
            time.sleep(wait)


_LIMITER = _RateLimiter(config.FRED_RATE_LIMIT)

_thread_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_thread_local, "fred_session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.fred_session = s
    return s


class FredConfigError(ValueError):
    """A misconfiguration (missing/invalid API key) — not worth retrying."""


def _require_key() -> str:
    key = config.FRED_API_KEY.strip()
    if not key:
        raise FredConfigError(
            "FRED_API_KEY is not set — get a free key at "
            "https://fred.stlouisfed.org/docs/api/api_key.html and set the "
            "FRED_API_KEY environment variable.")
    return key


def _get(path: str, params: dict) -> dict | None:
    """Rate-limited GET against a FRED endpoint. Returns parsed JSON or ``None``.

    Transient conditions (network, 429, 5xx) become :class:`RetryableError` so the
    manager's backoff/retry machinery handles them. A 400 that names the API key
    is a hard :class:`FredConfigError` (retrying won't fix a bad key); any other
    400 (unknown series, empty result) returns ``None`` so the caller can treat it
    as "no data for this entity" rather than a failure.
    """
    q = {"api_key": _require_key(), "file_type": "json"}
    q.update({k: v for k, v in params.items() if v not in (None, "")})

    _LIMITER.acquire()
    try:
        resp = _session().get(f"{FRED_BASE}/{path}", params=q,
                              headers=config.FRED_HEADERS,
                              timeout=config.REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise RetryableError(f"network error: {e}") from e

    if resp.status_code in (429,):
        raise RetryableError("rate limited (429)")
    if resp.status_code >= 500:
        raise RetryableError(f"server error {resp.status_code}")
    if resp.status_code == 400:
        # FRED reports business errors as 400 with an error_message body.
        msg = ""
        try:
            msg = (resp.json() or {}).get("error_message", "")
        except ValueError:
            msg = resp.text[:160]
        if "api_key" in msg.lower():
            raise FredConfigError(f"FRED rejected the API key: {msg}")
        return None  # unknown series / no matching data — caller decides
    if resp.status_code in (403,):
        raise FredConfigError("FRED returned 403 (locked or invalid API key)")
    if resp.status_code != 200:
        raise ValueError(f"fred {resp.status_code}: {resp.text[:160]}")

    try:
        return resp.json()
    except ValueError as e:
        raise RetryableError(f"bad json: {e}") from e


def _atomic_write_csv(path: str, header: list[str], rows: list[list]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Series id handling + discovery (release / category / search -> series ids).
# ---------------------------------------------------------------------------

def _clean_series_id(sid: str) -> str | None:
    """FRED ids are upper-case alphanumerics plus '_'; reject anything else
    (which also blocks path separators / traversal in the on-disk layout)."""
    s = str(sid).strip().upper()
    if s and all(c.isalnum() or c == "_" for c in s):
        return s
    return None


def _paged_series(path: str, params: dict, cap: int) -> list[str]:
    """Collect series ids from a paginated FRED listing endpoint, up to ``cap``.

    ``/release/series``, ``/category/series`` and ``/series/search`` all return a
    ``seriess`` array and page via limit/offset (max 1000 per page).
    """
    ids: list[str] = []
    offset = 0
    while len(ids) < cap:
        page_limit = min(1000, cap - len(ids))
        data = _get(path, {**params, "limit": page_limit, "offset": offset})
        if not data:
            break
        batch = data.get("seriess") or []
        for row in batch:
            sid = _clean_series_id(row.get("id", ""))
            if sid:
                ids.append(sid)
        if len(batch) < page_limit:
            break  # last page
        offset += len(batch)
    return ids


def _resolve_series(params: dict) -> list[str]:
    """Build the deduped, ordered list of series ids a job should fetch.

    Combines explicit ``series_ids`` with anything discovered from a
    ``release_id``, ``category_id`` or ``search_text``. Order is stable
    (explicit first, then discovery in API order) and capped at ``max_series``.
    """
    cap = int(params.get("max_series") or DEFAULT_MAX_SERIES)
    seen: set[str] = set()
    out: list[str] = []

    def _add(sid: str | None):
        if sid and sid not in seen and len(out) < cap:
            seen.add(sid)
            out.append(sid)

    for raw in (params.get("series_ids") or []):
        _add(_clean_series_id(raw))

    if params.get("release_id") and len(out) < cap:
        for sid in _paged_series("release/series",
                                 {"release_id": params["release_id"]}, cap):
            _add(sid)
    if params.get("category_id") and len(out) < cap:
        for sid in _paged_series("category/series",
                                 {"category_id": params["category_id"]}, cap):
            _add(sid)
    if params.get("search_text") and len(out) < cap:
        for sid in _paged_series(
                "series/search",
                {"search_text": params["search_text"],
                 "order_by": "popularity", "sort_order": "desc"}, cap):
            _add(sid)

    return out


# ---------------------------------------------------------------------------
# Output naming — observations are keyed by (units, frequency) so a levels pull
# and a year-over-year pull of the same series live side by side without
# clobbering each other, and updates.py can reconstruct the request from a name.
# ---------------------------------------------------------------------------

def _obs_filename(series_id: str, units: str, freq: str) -> str:
    return f"{series_id}_{units}_{freq or 'orig'}.csv"


def _obs_params(params: dict) -> tuple[str, str, str]:
    """Validate and normalise the (units, frequency, aggregation) triple."""
    units = str(params.get("units") or "lin").strip().lower()
    if units not in UNITS:
        raise FredConfigError(f"invalid FRED units '{units}'")
    freq = str(params.get("frequency") or "").strip().lower()
    if freq and freq not in FREQUENCIES:
        raise FredConfigError(f"invalid FRED frequency '{freq}'")
    agg = str(params.get("aggregation_method") or "avg").strip().lower()
    if agg not in AGG_METHODS:
        raise FredConfigError(f"invalid FRED aggregation_method '{agg}'")
    return units, freq, agg


# ---------------------------------------------------------------------------
# Job type: one task per series.
# ---------------------------------------------------------------------------

def fred_plan(params: dict) -> list[tuple[str, str]]:
    _require_key()  # fail the whole job early if the key is missing
    units, freq, _ = _obs_params(params)  # validate before fanning out
    tasks = []
    for sid in _resolve_series(params):
        label = f"{sid} ({units}{'/' + freq if freq else ''})"
        tasks.append((sid, label))
    return tasks


def fred_run(params: dict, key: str) -> tuple[str, str]:
    series_id = key
    units, freq, agg = _obs_params(params)

    out_dir = os.path.join(config.FRED_DIR, series_id)
    out_path = os.path.join(out_dir, _obs_filename(series_id, units, freq))
    if os.path.exists(out_path):
        return ("done", out_path)

    # ALFRED point-in-time support: a real-time window or explicit vintage dates
    # pulls each observation as it was *first* published. When in play we keep the
    # realtime_start/realtime_end columns so revisions stay distinguishable.
    rt_start = params.get("realtime_start") or None
    rt_end = params.get("realtime_end") or None
    vintage = params.get("vintage_dates") or None
    if isinstance(vintage, (list, tuple)):
        vintage = ",".join(str(v) for v in vintage)
    point_in_time = bool(rt_start or rt_end or vintage)

    data = _get("series/observations", {
        "series_id": series_id,
        "units": units,
        "frequency": freq or None,
        "aggregation_method": agg if freq else None,
        "observation_start": params.get("observation_start") or None,
        "observation_end": params.get("observation_end") or None,
        "realtime_start": rt_start,
        "realtime_end": rt_end,
        "vintage_dates": vintage,
        "sort_order": "asc",
    })
    if data is None:
        return ("skipped", "")  # unknown series id — nothing to retry
    observations = data.get("observations") or []
    if not observations:
        return ("skipped", "")  # series exists but no points in the window

    # FRED encodes a missing value as ".": carry it through as blank so the
    # column stays numeric for charting/backtests.
    if point_in_time:
        header = ["date", "value", "realtime_start", "realtime_end"]
        rows = [[o.get("date", ""),
                 "" if o.get("value") == "." else o.get("value", ""),
                 o.get("realtime_start", ""), o.get("realtime_end", "")]
                for o in observations]
    else:
        header = ["date", "value"]
        rows = [[o.get("date", ""),
                 "" if o.get("value") == "." else o.get("value", "")]
                for o in observations]
    _atomic_write_csv(out_path, header, rows)

    # One-row metadata sidecar (best-effort: never lose the observations we just
    # saved if the metadata call fails).
    meta_path = os.path.join(out_dir, f"{series_id}_meta.csv")
    if not os.path.exists(meta_path):
        try:
            _write_meta(series_id, meta_path)
        except (RetryableError, FredConfigError, ValueError, OSError):
            pass

    return ("done", out_path)


_META_FIELDS = ["id", "title", "units", "units_short", "frequency",
                "frequency_short", "seasonal_adjustment",
                "seasonal_adjustment_short", "observation_start",
                "observation_end", "last_updated", "popularity", "notes"]


def _write_meta(series_id: str, meta_path: str):
    data = _get("series", {"series_id": series_id})
    seriess = (data or {}).get("seriess") or []
    if not seriess:
        return
    s = seriess[0]
    row = [str(s.get(f, "")).replace("\n", " ").strip() for f in _META_FIELDS]
    _atomic_write_csv(meta_path, _META_FIELDS, [row])


# Registry merged into jobs.JOB_TYPES.
FRED_JOB_TYPES = {
    "fred": (fred_plan, fred_run, "FRED macro series"),
}
