"""Binance futures job types: the derivatives-native data a crypto quant needs.

The existing :mod:`app.jobs` Binance source pulls *spot* OHLCV. Futures research
lives or dies on data spot doesn't have — funding, open interest, and positioning
— so this module plugs the Binance **USD-M** (``fapi.binance.com``) and **COIN-M**
(``dapi.binance.com``) futures markets into the same plan/run task model the rest
of the manager uses. What makes it "sophisticated" rather than a flat klines grab:

* **Two markets, one code path** — USD-M (USDT/USDC-margined, e.g. ``BTCUSDT``)
  and COIN-M (coin-margined inverse, e.g. ``BTCUSD_PERP``) differ in host, symbol
  shape and how the /futures/data endpoints are addressed (``symbol`` vs
  ``pair``+``contractType``). A small :data:`MARKETS` table and a couple of
  helpers absorb the difference so every dataset works on both.
* **Four price series, not one** — besides tradeable OHLCV, the same kline engine
  fetches **mark-price**, **index-price** and **premium-index** klines (the basis
  and funding-prediction inputs), each into its own folder, one CSV per
  symbol-month so a long range resumes file-by-file.
* **Funding rate history** — the realised 8-hourly funding each perpetual paid,
  paged on ``startTime`` over the whole requested range.
* **Positioning & flow** — the Binance ``/futures/data`` family: **open
  interest** history, **top-trader** long/short (by accounts and by positions),
  **global** account long/short, and **taker** buy/sell volume. These are a
  rolling ~30-day window server-side and period-bucketed (5m … 1d); the runner
  pages them oldest-first and writes columns from the union of fields each
  endpoint returns (they differ), timestamp first.

Three job types are exposed, mirroring how the SEC source splits one request into
several products: ``binance_futures`` (klines family), ``binance_funding``
(funding history) and ``binance_metrics`` (the /futures/data sentiment set). The
API endpoint in :mod:`app.main` fans a single UI request out into whichever of the
three the user selected.

All futures traffic funnels through a shared rate limiter (:data:`_LIMITER`) so the
process stays under Binance's per-IP weight budget no matter how many worker
threads are downloading at once; transient conditions (network, 429/418, 5xx)
become :class:`RetryableError` for the manager's backoff/retry machinery, while a
400 (bad symbol/interval) is a permanent :class:`ValueError`.
"""
import csv
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

from . import config
from .jobs import RetryableError

# ---------------------------------------------------------------------------
# Markets. Each entry carries the two hosts a market uses: the versioned API
# base (klines / funding) and the (shared-host) /futures/data base for the
# sentiment endpoints. ``kind_path`` maps a price-series kind to its endpoint;
# ``index`` is keyed by ``pair`` not ``symbol``, the rest by ``symbol``.
# ---------------------------------------------------------------------------

MARKETS = {
    "usdm": {
        "label": "USD-M futures (fapi)",
        "api": "https://fapi.binance.com/fapi/v1",
        "data": "https://fapi.binance.com/futures/data",
    },
    "coinm": {
        "label": "COIN-M futures (dapi)",
        "api": "https://dapi.binance.com/dapi/v1",
        "data": "https://dapi.binance.com/futures/data",
    },
}

# Price-series kinds -> (endpoint, key-by-pair?, reduced-columns?). Mark/index/
# premium klines reuse the 12-field kline array but only OHLC is meaningful (the
# volume slots are zero), so those write a reduced column set. Index price klines
# are addressed by ``pair`` (BTCUSDT / BTCUSD), the others by ``symbol``.
KLINE_KINDS = {
    "ohlcv":   {"endpoint": "klines",             "by_pair": False, "ohlc_only": False, "dir": "klines"},
    "mark":    {"endpoint": "markPriceKlines",    "by_pair": False, "ohlc_only": True,  "dir": "mark"},
    "index":   {"endpoint": "indexPriceKlines",   "by_pair": True,  "ohlc_only": True,  "dir": "index"},
    "premium": {"endpoint": "premiumIndexKlines", "by_pair": False, "ohlc_only": True,  "dir": "premium"},
}

# /futures/data sentiment metrics. ``endpoint`` differs per market for taker flow
# (USD-M ``takerlongshortRatio`` vs COIN-M ``takerBuySellVol``); ``coinm_contract``
# flags the metrics whose COIN-M form needs a ``contractType`` alongside ``pair``.
METRICS = {
    "open_interest": {
        "usdm": "openInterestHist", "coinm": "openInterestHist",
        "coinm_contract": True, "label": "Open interest history",
    },
    "top_accounts": {
        "usdm": "topLongShortAccountRatio", "coinm": "topLongShortAccountRatio",
        "coinm_contract": False, "label": "Top trader long/short (accounts)",
    },
    "top_positions": {
        "usdm": "topLongShortPositionRatio", "coinm": "topLongShortPositionRatio",
        "coinm_contract": False, "label": "Top trader long/short (positions)",
    },
    "global_accounts": {
        "usdm": "globalLongShortAccountRatio", "coinm": "globalLongShortAccountRatio",
        "coinm_contract": False, "label": "Global long/short (accounts)",
    },
    "taker": {
        "usdm": "takerlongshortRatio", "coinm": "takerBuySellVol",
        "coinm_contract": True, "label": "Taker buy/sell volume",
    },
}

# Period buckets accepted by the /futures/data endpoints.
METRIC_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}

KLINE_LIMIT = 1500    # max rows per klines request (USD-M and COIN-M)
FUNDING_LIMIT = 1000  # max rows per fundingRate request
METRIC_LIMIT = 500    # max rows per /futures/data request

# The /futures/data endpoints only serve the most recent ~30 days. Requesting an
# older window just yields the available tail; we still bound the cursor so a job
# without dates fetches that whole rolling window rather than one 500-row page.
METRIC_MAX_DAYS = 30


# ---------------------------------------------------------------------------
# Shared rate limiter — every futures request goes through this so the process
# stays under Binance's per-IP weight budget. (Same design as app.cftc._LIMITER.)
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


_LIMITER = _RateLimiter(config.BINANCE_FUTURES_RATE_LIMIT)

_thread_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_thread_local, "futures_session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.futures_session = s
    return s


class FuturesConfigError(ValueError):
    """A misconfiguration (unknown market / kind / period) — not worth retrying."""


def _get(url: str, query: dict) -> list:
    """Rate-limited GET against a Binance futures endpoint, returning a row list.

    Transient conditions (network, 429/418, 5xx) become :class:`RetryableError`
    so the manager retries with backoff; a 400 is a permanent input error
    (unknown symbol/interval) and raises :class:`ValueError`.
    """
    q = {k: v for k, v in query.items() if v not in (None, "")}

    _LIMITER.acquire()
    try:
        resp = _session().get(url, params=q, timeout=config.REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise RetryableError(f"network error: {e}") from e

    if resp.status_code in (429, 418):  # rate limited / IP auto-banned — back off
        raise RetryableError(f"rate limited ({resp.status_code})")
    if resp.status_code >= 500:
        raise RetryableError(f"server error {resp.status_code}")
    if resp.status_code != 200:
        # 400 = bad symbol/interval/period: permanent, not worth retrying.
        raise ValueError(f"binance futures {resp.status_code}: {resp.text[:160]}")

    try:
        data = resp.json()
    except ValueError as e:
        raise RetryableError(f"bad json: {e}") from e
    return data if isinstance(data, list) else []


def _atomic_write_csv(path: str, header: list, rows: list):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".part"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp, path)  # atomic: a partial write never looks complete


# ---------------------------------------------------------------------------
# Validation / small helpers.
# ---------------------------------------------------------------------------

def _market(params: dict) -> str:
    m = str(params.get("market") or "usdm").strip().lower()
    if m not in MARKETS:
        raise FuturesConfigError(
            f"unknown market '{m}'; choose one of {', '.join(sorted(MARKETS))}")
    return m


def _clean_symbol(sym) -> str | None:
    """Futures symbols are upper-case alphanumerics, plus the single underscore
    COIN-M uses (``BTCUSD_PERP``, ``BTCUSD_240628``). Reject anything else, which
    also blocks path separators / traversal in the on-disk layout."""
    s = str(sym).strip().upper()
    if s and all(c.isalnum() or c == "_" for c in s):
        return s
    return None


def _pair(symbol: str) -> str:
    """Underlying pair for the ``pair``-keyed endpoints: COIN-M strips the
    contract suffix (``BTCUSD_PERP`` -> ``BTCUSD``); USD-M is already the pair."""
    return symbol.split("_", 1)[0]


def _symbols(params: dict) -> list[str]:
    out, seen = [], set()
    for raw in (params.get("symbols") or []):
        s = _clean_symbol(raw)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _day_ms(value: str, *, end: bool = False) -> int | None:
    """A ``YYYY-MM-DD`` job bound as epoch-ms UTC. ``end`` rolls to next midnight
    (exclusive upper bound). Returns ``None`` for blank/invalid input."""
    s = str(value or "").strip()
    if not s:
        return None
    try:
        d = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    if end:
        d += timedelta(days=1)
    return int(d.timestamp() * 1000)


def _iso(ms) -> str:
    """Epoch-ms -> ISO-8601 UTC with millisecond precision."""
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat(
        sep=" ", timespec="milliseconds")


def _months(start: str, end: str):
    """Yield 'YYYY-MM' for every calendar month touched by [start, end]."""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    y, m = s.year, s.month
    while (y, m) <= (e.year, e.month):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m > 12:
            m, y = 1, y + 1


def _write_market_marker(market: str, symbol: str):
    """One-row sidecar recording which market a dataset belongs to, so the
    incremental updater can re-issue the right requests (best-effort)."""
    path = os.path.join(config.FUTURES_DIR, symbol, "_market.csv")
    if os.path.exists(path):
        return
    try:
        _atomic_write_csv(path, ["market", "symbol", "pair", "label"],
                          [[market, symbol, _pair(symbol), MARKETS[market]["label"]]])
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Job type 1: price-series klines (OHLCV / mark / index / premium).
# One task per (symbol, kind, month) so a long range resumes file-by-file.
# ---------------------------------------------------------------------------

def _kinds(params: dict) -> list[str]:
    out, seen = [], set()
    for raw in (params.get("kinds") or ["ohlcv"]):
        k = str(raw).strip().lower()
        if k not in KLINE_KINDS:
            raise FuturesConfigError(
                f"unknown kline kind '{k}'; choose from {', '.join(KLINE_KINDS)}")
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def futures_klines_plan(params: dict) -> list[tuple[str, str]]:
    market = _market(params)        # fail the whole job early on a bad market…
    kinds = _kinds(params)          # …or a bad kind
    interval = str(params.get("interval") or "1h")
    start, end = params["start_date"], params["end_date"]
    tasks = []
    for sym in _symbols(params):
        for kind in kinds:
            for month in _months(start, end):
                key = f"{sym}/{kind}/{interval}/{month}"
                tasks.append((key, f"[{market}] {sym} {kind} {interval} {month}"))
    return tasks


def futures_klines_run(params: dict, key: str) -> tuple[str, str]:
    market = _market(params)
    sym, kind, interval, month = key.split("/")
    spec = KLINE_KINDS[kind]

    out_dir = os.path.join(config.FUTURES_DIR, sym, spec["dir"], interval)
    out_path = os.path.join(out_dir, f"{month}.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    # Window for this month, clamped to the job's overall range. Binance times are
    # epoch-ms UTC; startTime inclusive, endTime exclusive.
    y, mo = (int(x) for x in month.split("-"))
    month_start = datetime(y, mo, 1, tzinfo=timezone.utc)
    month_end = (datetime(y + 1, 1, 1, tzinfo=timezone.utc) if mo == 12
                 else datetime(y, mo + 1, 1, tzinfo=timezone.utc))
    win_start = max(int(month_start.timestamp() * 1000),
                    _day_ms(params["start_date"]) or 0)
    win_end = min(int(month_end.timestamp() * 1000),
                  _day_ms(params["end_date"], end=True) or 0)
    if win_start >= win_end:
        return ("skipped", "")

    url = f"{MARKETS[market]['api']}/{spec['endpoint']}"
    id_field = "pair" if spec["by_pair"] else "symbol"
    id_value = _pair(sym) if spec["by_pair"] else sym

    cursor = win_start
    end_ms = win_end
    rows = []
    while cursor < end_ms:
        batch = _get(url, {id_field: id_value, "interval": interval,
                           "startTime": cursor, "endTime": end_ms - 1,
                           "limit": KLINE_LIMIT})
        if not batch:
            break
        rows.extend(batch)
        last_open = batch[-1][0]
        cursor = last_open + 1  # advance past the last candle returned
        if len(batch) < KLINE_LIMIT:
            break

    if not rows:
        # No klines in this window (symbol not yet listed) — nothing to retry.
        return ("skipped", "")

    if spec["ohlc_only"]:
        header = ["open_time", "open", "high", "low", "close", "close_time"]
        table = [[_iso(k[0]), k[1], k[2], k[3], k[4], _iso(k[6])] for k in rows]
    else:
        header = ["open_time", "open", "high", "low", "close", "volume",
                  "close_time", "quote_volume", "trades",
                  "taker_buy_base", "taker_buy_quote"]
        table = [[_iso(k[0]), k[1], k[2], k[3], k[4], k[5], _iso(k[6]),
                  k[7], k[8], k[9], k[10]] for k in rows]
    _atomic_write_csv(out_path, header, table)
    _write_market_marker(market, sym)
    return ("done", out_path)


# ---------------------------------------------------------------------------
# Job type 2: funding-rate history. One task per symbol (the whole requested
# range, paged on startTime — funding is only 3x/day so it stays small).
# ---------------------------------------------------------------------------

def futures_funding_plan(params: dict) -> list[tuple[str, str]]:
    market = _market(params)
    return [(sym, f"[{market}] {sym} funding") for sym in _symbols(params)]


def futures_funding_run(params: dict, key: str) -> tuple[str, str]:
    market = _market(params)
    sym = _clean_symbol(key)
    if not sym:
        return ("skipped", "")

    out_dir = os.path.join(config.FUTURES_DIR, sym)
    out_path = os.path.join(out_dir, "funding.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    url = f"{MARKETS[market]['api']}/fundingRate"
    cursor = _day_ms(params.get("start_date")) or 0
    end_ms = _day_ms(params.get("end_date"), end=True)

    rows: list[dict] = []
    while True:
        batch = _get(url, {"symbol": sym, "startTime": cursor,
                           "endTime": (end_ms - 1) if end_ms else None,
                           "limit": FUNDING_LIMIT})
        if not batch:
            break
        rows.extend(batch)
        last = int(batch[-1]["fundingTime"])
        cursor = last + 1
        if len(batch) < FUNDING_LIMIT or (end_ms and cursor >= end_ms):
            break

    if not rows:
        return ("skipped", "")  # funding not published for this symbol/window

    header = ["funding_time", "funding_rate", "mark_price"]
    table = [[_iso(r["fundingTime"]), r.get("fundingRate", ""),
              r.get("markPrice", "")] for r in rows]
    _atomic_write_csv(out_path, header, table)
    _write_market_marker(market, sym)
    return ("done", out_path)


# ---------------------------------------------------------------------------
# Job type 3: /futures/data sentiment metrics (open interest, long/short
# ratios, taker flow). One task per (symbol, metric); rolling ~30-day window.
# ---------------------------------------------------------------------------

def _metrics(params: dict) -> list[str]:
    out, seen = [], set()
    for raw in (params.get("metrics") or []):
        m = str(raw).strip().lower()
        if m not in METRICS:
            raise FuturesConfigError(
                f"unknown metric '{m}'; choose from {', '.join(METRICS)}")
        if m not in seen:
            seen.add(m)
            out.append(m)
    return out


def _period(params: dict) -> str:
    p = str(params.get("period") or "5m").strip()
    if p not in METRIC_PERIODS:
        raise FuturesConfigError(
            f"unknown period '{p}'; choose from {', '.join(sorted(METRIC_PERIODS))}")
    return p


def futures_metrics_plan(params: dict) -> list[tuple[str, str]]:
    market = _market(params)
    metrics = _metrics(params)
    period = _period(params)
    tasks = []
    for sym in _symbols(params):
        for metric in metrics:
            key = f"{sym}/{metric}"
            tasks.append((key, f"[{market}] {sym} {METRICS[metric]['label']} ({period})"))
    return tasks


def _metric_query(market: str, metric: str, symbol: str, period: str,
                  contract_type: str) -> dict:
    """Identity params for one /futures/data request, market-shaped: USD-M keys by
    ``symbol``; COIN-M keys by ``pair`` and adds ``contractType`` where required."""
    if market == "coinm":
        q = {"pair": _pair(symbol), "period": period}
        if METRICS[metric]["coinm_contract"]:
            q["contractType"] = contract_type
        return q
    return {"symbol": symbol, "period": period}


def futures_metrics_run(params: dict, key: str) -> tuple[str, str]:
    market = _market(params)
    period = _period(params)
    contract_type = str(params.get("contract_type") or "PERPETUAL").strip().upper()
    sym, _, metric = key.partition("/")
    sym = _clean_symbol(sym)
    if not sym or metric not in METRICS:
        return ("skipped", "")

    out_dir = os.path.join(config.FUTURES_DIR, sym, "metrics")
    out_path = os.path.join(out_dir, f"{metric}_{period}.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    url = f"{MARKETS[market]['data']}/{METRICS[metric][market]}"
    ident = _metric_query(market, metric, sym, period, contract_type)

    # Bound the cursor to the rolling 30-day window the endpoint serves, narrowed
    # by any job dates. Page oldest-first on startTime, like the kline engine.
    floor = int((datetime.now(timezone.utc) - timedelta(days=METRIC_MAX_DAYS))
                .timestamp() * 1000)
    cursor = max(floor, _day_ms(params.get("start_date")) or 0)
    end_ms = _day_ms(params.get("end_date"), end=True)

    rows: list[dict] = []
    seen_ts: set = set()
    while True:
        batch = _get(url, {**ident, "startTime": cursor,
                           "endTime": (end_ms - 1) if end_ms else None,
                           "limit": METRIC_LIMIT})
        # Drop rows already collected (windows can overlap on the boundary ts).
        fresh = [r for r in batch if r.get("timestamp") not in seen_ts]
        for r in fresh:
            seen_ts.add(r.get("timestamp"))
        rows.extend(fresh)
        if len(batch) < METRIC_LIMIT or not batch:
            break
        last_ts = int(batch[-1]["timestamp"])
        nxt = last_ts + 1
        if nxt <= cursor or (end_ms and nxt >= end_ms):
            break
        cursor = nxt

    if not rows:
        # Outside the served 30-day window, or symbol has no sentiment data.
        return ("skipped", "")

    rows.sort(key=lambda r: int(r.get("timestamp", 0)))
    header, table = _shape_metric_rows(rows)
    _atomic_write_csv(out_path, header, table)
    _write_market_marker(market, sym)
    return ("done", out_path)


# Identity columns pulled to the front of a metrics CSV when present; the rest
# (the metric-specific value fields, which differ per endpoint) follow in
# first-seen order — the same union-of-keys approach app.cftc uses for its
# family-dependent tables.
_METRIC_FRONT = ["time", "symbol", "pair", "contractType"]


def _shape_metric_rows(rows: list[dict]) -> tuple[list[str], list[list]]:
    """Turn raw metric dicts into (header, table): the epoch-ms ``timestamp`` is
    rendered as an ISO ``time`` column up front, then the union of remaining keys
    in first-seen order (endpoints omit/rename fields, so columns are dynamic)."""
    present: set = set()
    for r in rows:
        present.update(r.keys())
    cols, seen = [], set()
    for c in _METRIC_FRONT:
        if c == "time" or c in present:
            cols.append(c)
            seen.add(c)
    for r in rows:
        for k in r.keys():
            if k in seen or k == "timestamp":  # timestamp becomes 'time'
                continue
            cols.append(k)
            seen.add(k)

    table = []
    for r in rows:
        out = []
        for c in cols:
            if c == "time":
                out.append(_iso(r.get("timestamp", 0)))
            else:
                v = r.get(c, "")
                out.append("" if v is None else v)
        table.append(out)
    return cols, table


# Registry merged into jobs.JOB_TYPES.
FUTURES_JOB_TYPES = {
    "binance_futures": (futures_klines_plan, futures_klines_run,
                        "Binance futures klines (OHLCV/mark/index/premium)"),
    "binance_funding": (futures_funding_plan, futures_funding_run,
                        "Binance futures funding-rate history"),
    "binance_metrics": (futures_metrics_plan, futures_metrics_run,
                        "Binance futures open interest / long-short / taker flow"),
}
