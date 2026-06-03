"""CFTC job type: Commitments of Traders positioning from the CFTC.

Plugs the CFTC's weekly Commitments of Traders (COT) reports into the same
plan/run task model the rest of the manager uses (see :mod:`app.jobs`). One job
type, ``cftc``, with one resumable task per futures market (keyed by its CFTC
contract market code). What makes it "sophisticated" rather than a flat CSV grab:

* **Seven report families** — the same markets are published under three trader
  classification schemes, each in a futures-only and a futures+options
  ("combined") flavour, plus the supplemental commodity-index report:
    - *legacy* — commercial / non-commercial / non-reportable
    - *disaggregated* — producer-merchant / swap / managed-money / other
    - *traders in financial futures (TFF)* — dealer / asset-manager / leveraged
  A job targets one family via ``report``; the API endpoint fans a multi-family
  request out into one job each, the way the SEC endpoint does per product.
* **Discovery** — a job needn't list contract codes by hand. The planner can
  expand a *commodity group* ("FINANCIAL INSTRUMENTS"), a *subgroup*
  ("CURRENCY"), a free-text *market filter* ("crude oil"), or pull the *entire*
  report, dedup the lot against any explicit codes, and fan out one task per
  market.
* **Server-side SoQL** — each market's full weekly history comes from the CFTC's
  Socrata API filtered (``$where``), ordered (``$order``) and paged
  (``$limit``/``$offset``) server-side, optionally bounded to a date window, so a
  backtest reads exactly the slice it asked for. Columns are written
  dynamically: each report family carries a different set of trader buckets, and
  Socrata omits null fields per row, so the writer takes the union of keys seen
  (id/date/market columns first) rather than hard-coding 100+ headers per family.
* **Metadata** — every market also gets a one-row ``_meta.csv`` (market name,
  commodity, group/subgroup, contract units, CFTC codes, and the span/observation
  count actually on disk).

The CFTC API is keyless but throttles anonymous traffic; an optional Socrata app
token (``CFTC_APP_TOKEN``) lifts the limit and is sent on every request. All CFTC
traffic funnels through a shared limiter (:data:`_LIMITER`) so the whole process
stays polite no matter how many worker threads are downloading at once.
"""
import csv
import os
import threading
import time
from datetime import datetime

import requests

from . import config
from .jobs import RetryableError

# Socrata resource endpoints live under one host; each report family is a
# dataset addressed by its 4x4 identifier. (https://publicreporting.cftc.gov/)
CFTC_BASE = "https://publicreporting.cftc.gov/resource"

# report key -> (Socrata dataset id, human label). The key doubles as the
# on-disk filename suffix, so a market's legacy and disaggregated histories sit
# side by side without clobbering each other.
REPORTS = {
    "legacy_fut":       ("6dca-aqww", "Legacy (futures only)"),
    "legacy_combined":  ("jun7-fc8e", "Legacy (futures + options)"),
    "disagg_fut":       ("72hh-3qpy", "Disaggregated (futures only)"),
    "disagg_combined":  ("kh3c-gbw2", "Disaggregated (futures + options)"),
    "tff_fut":          ("gpe5-46if", "Traders in Financial Futures (futures only)"),
    "tff_combined":     ("yw9f-hn96", "Traders in Financial Futures (futures + options)"),
    "supplemental":     ("4zgm-a668", "Supplemental commodity index"),
}

DATE_FIELD = "report_date_as_yyyy_mm_dd"
CODE_FIELD = "cftc_contract_market_code"

# Identity / descriptive columns pulled to the front of every CSV (when present);
# everything else (the report-family-specific position buckets) follows in the
# order the API returns it. Keeps the wide, family-dependent tables readable.
_FRONT = [
    DATE_FIELD, "market_and_exchange_names", CODE_FIELD,
    "cftc_market_code", "cftc_region_code", "cftc_commodity_code",
    "commodity_name", "commodity", "commodity_group_name",
    "commodity_subgroup_name", "contract_units", "futonly_or_combined",
    "open_interest_all",
]

# Socrata's own row-id and any ``:`` system columns add nothing for analysis.
_DROP_COLUMNS = {"id"}

# Descriptive fields copied into the one-row _meta.csv sidecar, plus three
# derived span columns appended by _write_meta.
_META_FIELDS = [
    CODE_FIELD, "market_and_exchange_names", "cftc_market_code",
    "cftc_region_code", "cftc_commodity_code", "commodity_name", "commodity",
    "commodity_group_name", "commodity_subgroup_name", "contract_units",
]

# Socrata caps a single page at 50000 rows; a market's whole weekly history
# (legacy goes back to 1986 ≈ 2000 rows) fits in one page, but we still loop on
# offset so nothing is silently truncated.
PAGE_SIZE = 50000

# Safety cap on how many markets one discovery (group/subgroup/filter/all) may
# fan out into. Override per job with params["max_markets"].
DEFAULT_MAX_MARKETS = 1000


# ---------------------------------------------------------------------------
# Shared rate limiter — every CFTC request goes through this so the process
# stays polite to the Socrata host. (Same design as app.fred._RateLimiter.)
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


_LIMITER = _RateLimiter(config.CFTC_RATE_LIMIT)

_thread_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_thread_local, "cftc_session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.cftc_session = s
    return s


class CftcConfigError(ValueError):
    """A misconfiguration (unknown report / bad date) — not worth retrying."""


def _headers() -> dict:
    h = dict(config.CFTC_HEADERS)
    token = config.CFTC_APP_TOKEN.strip()
    if token:
        # Socrata raises the throttle ceiling for requests carrying an app token.
        h["X-App-Token"] = token
    return h


def _get(dataset_id: str, query: dict) -> list:
    """Rate-limited SoQL GET against a CFTC Socrata dataset. Returns a row list.

    Transient conditions (network, 429, 5xx) become :class:`RetryableError` so
    the manager's backoff/retry machinery handles them. A 400 is a malformed
    query (programmer error, not worth retrying) and raises ``ValueError``.
    """
    q = {k: v for k, v in query.items() if v not in (None, "")}

    _LIMITER.acquire()
    try:
        resp = _session().get(f"{CFTC_BASE}/{dataset_id}.json", params=q,
                              headers=_headers(), timeout=config.REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise RetryableError(f"network error: {e}") from e

    if resp.status_code == 429:
        raise RetryableError("rate limited (429)")
    if resp.status_code >= 500:
        raise RetryableError(f"server error {resp.status_code}")
    if resp.status_code == 400:
        # Socrata returns a JSON error body for a bad SoQL query.
        msg = ""
        try:
            msg = (resp.json() or {}).get("message", "")
        except ValueError:
            msg = resp.text[:160]
        raise ValueError(f"CFTC rejected the query: {msg or resp.text[:160]}")
    if resp.status_code != 200:
        raise ValueError(f"cftc {resp.status_code}: {resp.text[:160]}")

    try:
        data = resp.json()
    except ValueError as e:
        raise RetryableError(f"bad json: {e}") from e
    return data if isinstance(data, list) else []


def _atomic_write_csv(path: str, header: list[str], rows: list[list]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Parameter validation + SoQL helpers.
# ---------------------------------------------------------------------------

def _require_report(params: dict) -> str:
    report = str(params.get("report") or "legacy_fut").strip().lower()
    if report not in REPORTS:
        raise CftcConfigError(
            f"unknown CFTC report '{report}'; choose one of "
            f"{', '.join(sorted(REPORTS))}")
    return report


def _clean_code(code: str) -> str | None:
    """CFTC contract market codes are upper-case alphanumerics (e.g. ``001602``,
    ``13874A``); reject anything else (which also blocks path separators /
    traversal in the on-disk layout and SoQL injection in ``$where``)."""
    s = str(code).strip().upper()
    if s and all(c.isalnum() for c in s):
        return s
    return None


def _iso_datetime(value: str) -> str | None:
    """Validate a ``YYYY-MM-DD`` input and return a Socrata floating-timestamp
    literal. Returns ``None`` for blank/invalid input so a typo can't smuggle
    SoQL into the ``$where`` clause."""
    s = str(value or "").strip()
    if not s:
        return None
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return None
    return f"{s}T00:00:00.000"


def _soql_literal(value: str) -> str:
    """Upper-case a discovery filter value and escape it for a SoQL string
    literal (single quotes doubled), matching the upper-cased data."""
    return str(value).strip().upper().replace("'", "''")


def _date_window(params: dict) -> str:
    """Build the optional ``report_date`` bounds for a ``$where`` clause."""
    clauses = []
    lo = _iso_datetime(params.get("start_date"))
    hi = _iso_datetime(params.get("end_date"))
    if lo:
        clauses.append(f"{DATE_FIELD} >= '{lo}'")
    if hi:
        clauses.append(f"{DATE_FIELD} <= '{hi}'")
    return " AND ".join(clauses)


# ---------------------------------------------------------------------------
# Discovery: commodity group / subgroup / market text -> contract codes.
# ---------------------------------------------------------------------------

def _discover_markets(report: str, params: dict, cap: int) -> list[dict]:
    """Group a report family down to its distinct markets, filtered server-side
    by commodity group/subgroup and client-side by a market-name substring."""
    dataset_id = REPORTS[report][0]
    where = []
    if params.get("commodity_group"):
        where.append(f"upper(commodity_group_name)='"
                     f"{_soql_literal(params['commodity_group'])}'")
    if params.get("commodity_subgroup"):
        where.append(f"upper(commodity_subgroup_name)='"
                     f"{_soql_literal(params['commodity_subgroup'])}'")
    cols = (f"{CODE_FIELD},market_and_exchange_names,"
            f"commodity_subgroup_name,commodity_group_name")
    data = _get(dataset_id, {
        "$select": cols,
        "$group": cols,
        "$order": CODE_FIELD,
        "$where": " AND ".join(where) or None,
        "$limit": PAGE_SIZE,
    })

    market_filter = str(params.get("market_filter") or "").strip().lower()
    out: list[dict] = []
    seen: set[str] = set()
    for row in data:
        code = _clean_code(row.get(CODE_FIELD, ""))
        if not code or code in seen:
            continue
        market = row.get("market_and_exchange_names", "")
        if market_filter and market_filter not in market.lower():
            continue
        seen.add(code)
        out.append({"code": code, "market": market})
        if len(out) >= cap:
            break
    return out


def _resolve_markets(report: str, params: dict) -> list[tuple[str, str]]:
    """Build the deduped, ordered (code, market) list a job should fetch.

    Combines explicit ``contract_codes`` with anything discovered from a
    commodity group/subgroup, a market-name filter, or ``all_markets``. Order is
    stable (explicit first, then discovery in code order) and capped at
    ``max_markets``."""
    cap = int(params.get("max_markets") or DEFAULT_MAX_MARKETS)
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(code: str | None, market: str):
        if code and code not in seen and len(out) < cap:
            seen.add(code)
            out.append((code, market or ""))

    for raw in (params.get("contract_codes") or []):
        _add(_clean_code(raw), "")

    wants_discovery = (params.get("all_markets") or params.get("commodity_group")
                       or params.get("commodity_subgroup")
                       or params.get("market_filter"))
    if wants_discovery and len(out) < cap:
        for m in _discover_markets(report, params, cap):
            _add(m["code"], m["market"])

    return out


# ---------------------------------------------------------------------------
# Output shaping — wide, family-dependent tables written from the union of keys.
# ---------------------------------------------------------------------------

def _clean_date(value) -> str:
    """Socrata floating timestamps come back as ``2024-01-02T00:00:00.000``;
    keep just the calendar date for a tidy, joinable column."""
    s = str(value or "")
    return s.split("T", 1)[0] if "T" in s else s


def _order_columns(rows: list[dict]) -> list[str]:
    """Ordered union of keys across ``rows``: identity/date columns first, then
    every other field in first-seen order (Socrata drops null keys per row, so
    different rows can carry different keys)."""
    present: set[str] = set()
    for r in rows:
        present.update(r.keys())
    cols: list[str] = []
    seen: set[str] = set()
    for c in _FRONT:
        if c in present:
            cols.append(c)
            seen.add(c)
    for r in rows:
        for k in r.keys():
            if k in seen or k in _DROP_COLUMNS or k.startswith(":"):
                continue
            cols.append(k)
            seen.add(k)
    return cols


def _fetch_series(dataset_id: str, code: str, params: dict) -> list[dict]:
    """Every weekly report for one contract code, oldest first, paged on offset
    and bounded to the job's optional date window."""
    where = f"{CODE_FIELD}='{code}'"
    window = _date_window(params)
    if window:
        where = f"{where} AND {window}"

    rows: list[dict] = []
    offset = 0
    while True:
        batch = _get(dataset_id, {
            "$where": where,
            "$order": f"{DATE_FIELD} ASC",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        })
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += len(batch)
    return rows


# ---------------------------------------------------------------------------
# Job type: one task per contract market code.
# ---------------------------------------------------------------------------

def cftc_plan(params: dict) -> list[tuple[str, str]]:
    report = _require_report(params)  # fail the whole job early on a bad report
    tasks = []
    for code, market in _resolve_markets(report, params):
        label = f"{code} {market}".strip() if market else code
        tasks.append((code, f"[{report}] {label}"))
    return tasks


def cftc_run(params: dict, key: str) -> tuple[str, str]:
    report = _require_report(params)
    code = _clean_code(key)
    if not code:
        return ("skipped", "")  # malformed code — nothing to fetch
    dataset_id = REPORTS[report][0]

    out_dir = os.path.join(config.CFTC_DIR, code)
    out_path = os.path.join(out_dir, f"{code}_{report}.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    rows = _fetch_series(dataset_id, code, params)
    if not rows:
        return ("skipped", "")  # no reports for this code in the window

    columns = _order_columns(rows)
    table = []
    for r in rows:
        out = []
        for c in columns:
            v = r.get(c, "")
            out.append(_clean_date(v) if c == DATE_FIELD else ("" if v is None else v))
        table.append(out)
    _atomic_write_csv(out_path, columns, table)

    # One-row metadata sidecar (best-effort: never lose the observations we just
    # saved if the metadata write fails).
    meta_path = os.path.join(out_dir, f"{code}_meta.csv")
    if not os.path.exists(meta_path):
        try:
            _write_meta(rows, meta_path)
        except (ValueError, OSError):
            pass

    return ("done", out_path)


def _write_meta(rows: list[dict], meta_path: str):
    """Descriptive fields from the most recent report, plus the on-disk span."""
    latest = rows[-1]  # rows are ascending by date
    dates = [_clean_date(r.get(DATE_FIELD)) for r in rows if r.get(DATE_FIELD)]
    header = _META_FIELDS + ["first_report", "last_report", "report_count"]
    row = [str(latest.get(f, "")).replace("\n", " ").strip() for f in _META_FIELDS]
    row += [min(dates) if dates else "", max(dates) if dates else "", len(rows)]
    _atomic_write_csv(meta_path, header, [row])


# Registry merged into jobs.JOB_TYPES.
CFTC_JOB_TYPES = {
    "cftc": (cftc_plan, cftc_run, "CFTC Commitments of Traders"),
}
