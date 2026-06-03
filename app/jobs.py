"""Job-type definitions: how to enumerate the resumable tasks of a job and how
to execute one task.

Each job type exposes two callables used by the manager:

* ``plan(params) -> list[(key, label)]`` — enumerate the unit tasks. ``key`` is
  stable and unique within the job so re-planning a resumed job is idempotent.
* ``run(params, key) -> (status, output)`` — perform one task. Returns a task
  status of ``done`` (work completed) or ``skipped`` (nothing to do, e.g. a
  market-closed hour). Raises :class:`RetryableError` for transient failures
  (dropped connections, timeouts, 5xx) that should be retried automatically.

The download/decoding logic mirrors the original ``scrape.py`` / ``process.py``
but is sliced per-file so progress survives interruptions.
"""
import csv
import lzma
import os
import struct
import threading
from datetime import datetime, timedelta, timezone

import requests

from . import config


class RetryableError(Exception):
    """A transient failure that should be retried (network drop, timeout, 5xx)."""


# A pooled requests.Session per worker thread. Reusing the session keeps the
# TCP/TLS connection to Dukascopy alive across the thousands of hour-file
# requests each thread makes, instead of paying a fresh handshake every time.
_thread_local = threading.local()


def _session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _thread_local.session = s
    return s


def _daterange(start: str, end: str):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    for i in range((e - s).days + 1):
        yield s + timedelta(days=i)


# ---------------------------------------------------------------------------
# Dukascopy .bi5 tick downloads
# ---------------------------------------------------------------------------

def dukascopy_plan(params: dict) -> list[tuple[str, str]]:
    ticker = params["ticker"]

    # "Fill gaps" jobs pass an explicit list of hour keys to (re)fetch instead
    # of a date range; the keys are already in the Dukascopy path form.
    only = params.get("only_keys")
    if only:
        tasks = []
        for key in only:
            date_part, _, hour_str = key.rpartition("/")
            label = f"{ticker} {date_part.replace('/', '-')} {hour_str}h"
            tasks.append((key, label))
        return tasks

    hmin = int(params.get("hour_min", 0))
    hmax = int(params.get("hour_max", 23))
    tasks = []
    for day in _daterange(params["start_date"], params["end_date"]):
        for hour in range(hmin, hmax + 1):
            # key encodes the exact Dukascopy path component.
            key = f"{day.strftime('%Y/%m/%d')}/{hour:02d}"
            label = f"{ticker} {day.strftime('%Y-%m-%d')} {hour:02d}h"
            tasks.append((key, label))
    return tasks


def dukascopy_run(params: dict, key: str) -> tuple[str, str]:
    ticker = params["ticker"]
    date_part, _, hour_str = key.rpartition("/")
    hour = int(hour_str)
    y, m, d = date_part.split("/")

    out_dir = os.path.join(config.RESULTS_DIR, ticker, f"{y}_{m}_{d}")
    os.makedirs(out_dir, exist_ok=True)
    filepath = os.path.join(out_dir, f"{hour:02d}h.bi5")

    # Already on disk (e.g. crash after writing the file but before the DB
    # commit): treat as done instead of re-downloading.
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        return ("done", filepath)

    url = (f"https://datafeed.dukascopy.com/datafeed/{ticker}/"
           f"{date_part}/{hour:02d}h_ticks.bi5")
    try:
        resp = _session().get(url, headers=config.DUKAS_HEADERS,
                              timeout=config.REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as e:
        raise RetryableError(f"network error: {e}") from e

    if resp.status_code == 404:
        # Dukascopy has no file for this hour (weekend / holiday / pre-listing).
        return ("skipped", "")
    if resp.status_code >= 500:
        raise RetryableError(f"server error {resp.status_code}")
    if resp.status_code != 200:
        raise RetryableError(f"unexpected status {resp.status_code}")

    tmp = filepath + ".part"
    with open(tmp, "wb") as f:
        f.write(resp.content)
    os.replace(tmp, filepath)  # atomic: a partial write never looks complete
    return ("done", filepath)


# ---------------------------------------------------------------------------
# Decode .bi5 -> CSV
# ---------------------------------------------------------------------------

def _price_scale(ticker: str) -> float:
    """Divisor that turns Dukascopy's integer price points into real prices.

    Dukascopy stores prices as integers scaled by ``10**digits``; ``digits`` is
    3 for JPY-quoted FX pairs (GBPJPY, USDJPY, ...) and 5 for the other FX
    pairs. Using 1e5 for a JPY pair shifts every price by 100x, so the quote
    currency must drive the scale. Exotic (non-FX) instruments may need their
    own entry.
    """
    return 1e3 if ticker.upper().endswith("JPY") else 1e5


def _decode_bi5(file_path: str, scale: float):
    if os.stat(file_path).st_size == 0:
        return []
    with lzma.open(file_path, "rb") as f:
        data = f.read()
    ticks = []
    for i in range(0, len(data), 20):
        if i + 20 > len(data):
            break
        ts, ask, bid, ask_vol, bid_vol = struct.unpack(">IIIff", data[i:i + 20])
        ticks.append((ts, ask / scale, bid / scale, ask_vol, bid_vol))
    return ticks


def decode_plan(params: dict) -> list[tuple[str, str]]:
    ticker = params["ticker"]
    src = os.path.join(config.RESULTS_DIR, ticker)
    if not os.path.isdir(src):
        return []
    tasks = []
    for folder in sorted(os.listdir(src)):
        if os.path.isdir(os.path.join(src, folder)):
            tasks.append((folder, f"{ticker} {folder}"))
    return tasks


def decode_run(params: dict, key: str) -> tuple[str, str]:
    ticker = params["ticker"]
    folder = key
    src = os.path.join(config.RESULTS_DIR, ticker, folder)
    try:
        day = datetime.strptime(folder, "%Y_%m_%d")
    except ValueError:
        return ("skipped", "")
    if not os.path.isdir(src):
        return ("skipped", "")

    out_dir = os.path.join(config.CSV_DIR, ticker)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{folder}.csv")

    scale = _price_scale(ticker)
    all_ticks = []
    for fname in sorted(os.listdir(src)):
        if not fname.endswith(".bi5"):
            continue
        hour_str = os.path.splitext(fname)[0]
        if not hour_str.endswith("h"):
            continue
        try:
            hour = int(hour_str[:-1])
        except ValueError:
            continue
        # Dukascopy tick timestamps are UTC; carry that through explicitly.
        start_time = day.replace(hour=hour, tzinfo=timezone.utc)
        for ts, ask, bid, ask_vol, bid_vol in _decode_bi5(os.path.join(src, fname), scale):
            dt = start_time + timedelta(milliseconds=ts)
            all_ticks.append((ts, dt, ask, bid, ask_vol, bid_vol))

    all_ticks.sort(key=lambda x: x[1])
    tmp = out_path + ".part"
    with open(tmp, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "datetime", "ask", "bid",
                        "ask_volume", "bid_volume"])
        for ts, dt, ask, bid, ask_vol, bid_vol in all_ticks:
            # ISO-8601 with millisecond precision and explicit +00:00 (UTC).
            writer.writerow([ts, dt.isoformat(sep=" ", timespec="milliseconds"),
                            ask, bid, ask_vol, bid_vol])
    os.replace(tmp, out_path)
    return ("done", out_path)


# ---------------------------------------------------------------------------
# yfinance company metadata
# ---------------------------------------------------------------------------

def _valid_ticker(t: str) -> bool:
    # Allow alphanumerics plus the '.'/'-' that appear in real symbols
    # (BRK.B, BF.B, RDS-A) rather than letters only.
    return bool(t) and all(c.isalnum() or c in ".-" for c in t)


def yfinance_plan(params: dict) -> list[tuple[str, str]]:
    tickers = params.get("tickers") or []
    seen = set()
    tasks = []
    for t in tickers:
        t = str(t).strip().upper()
        if _valid_ticker(t) and t not in seen:
            seen.add(t)
            tasks.append((t, t))
    return tasks


def _dedup_series(series, value_label: str) -> list[dict]:
    """Collapse runs of equal values, keeping the date each value changed."""
    rows, last = [], None
    for date in series.index:
        cur = series[date]
        if cur != last:
            rows.append({"Date": date, value_label: cur})
            last = cur
    return rows


def yfinance_run(params: dict, key: str) -> tuple[str, str]:
    import yfinance as yf  # imported lazily; heavy module

    ticker = key
    interval = str(params.get("interval") or "1d")
    start = params.get("start_date") or None
    end = params.get("end_date") or None

    out_dir = os.path.join(config.COMPANY_DIR, ticker)
    # The OHLCV file (the thing you actually backtest on) is the resume marker:
    # interval is in the name so different bar sizes don't collide.
    prices_path = os.path.join(out_dir, f"{ticker}_prices_{interval}.csv")
    if os.path.exists(prices_path):
        return ("done", prices_path)

    try:
        tk = yf.Ticker(ticker)
        if start or end:
            hist = tk.history(start=start, end=end, interval=interval)
        else:
            hist = tk.history(period="max", interval=interval)
    except Exception as e:  # noqa: BLE001 - yfinance raises many error types
        raise RetryableError(f"yfinance error: {e}") from e

    if hist is None or hist.empty:
        return ("skipped", "")

    import pandas as pd

    os.makedirs(out_dir, exist_ok=True)

    # --- the OHLCV bars themselves (previously fetched and thrown away) ---
    cols = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in hist.columns]
    prices = hist[cols].copy()
    prices.index.name = "Date"
    tmp = prices_path + ".part"
    prices.to_csv(tmp)
    os.replace(tmp, prices_path)  # atomic: a partial write never looks complete

    # --- metadata (best-effort: never lose the prices we just saved) ---
    info_path = os.path.join(out_dir, f"{ticker}_company_info.csv")
    if not os.path.exists(info_path):
        try:
            name = tk.info.get("longName", ticker)
        except Exception:  # noqa: BLE001 - .info can fail independently of history
            name = ticker
        pd.DataFrame([{"Ticker": ticker, "Name": name,
                       "First Date": hist.index[0]}]).to_csv(info_path, index=False)

    try:
        div_rows = _dedup_series(tk.dividends, "Dividend")
        if div_rows:
            pd.DataFrame(div_rows).to_csv(
                os.path.join(out_dir, f"{ticker}_dividends.csv"), index=False)
        split_rows = _dedup_series(tk.splits, "Stock Split")
        if split_rows:
            pd.DataFrame(split_rows).to_csv(
                os.path.join(out_dir, f"{ticker}_stock_splits.csv"), index=False)
    except Exception:  # noqa: BLE001 - corporate-action fetch is optional extra
        pass

    return ("done", prices_path)


# ---------------------------------------------------------------------------
# Binance crypto OHLCV (klines) — public API, no key required
# ---------------------------------------------------------------------------

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


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


def binance_plan(params: dict) -> list[tuple[str, str]]:
    symbols = params.get("symbols") or []
    interval = str(params.get("interval") or "1d")
    start, end = params["start_date"], params["end_date"]
    seen = set()
    tasks = []
    for sym in symbols:
        sym = str(sym).strip().upper()
        if not (sym and sym.isalnum()) or sym in seen:
            continue
        seen.add(sym)
        # One task per symbol-month so a long range resumes file-by-file.
        for month in _months(start, end):
            key = f"{sym}/{interval}/{month}"
            tasks.append((key, f"{sym} {interval} {month}"))
    return tasks


def binance_run(params: dict, key: str) -> tuple[str, str]:
    sym, interval, month = key.split("/")

    out_dir = os.path.join(config.CRYPTO_DIR, sym, interval)
    out_path = os.path.join(out_dir, f"{month}.csv")
    if os.path.exists(out_path):
        return ("done", out_path)

    # Window for this month, clamped to the job's overall date range. Binance
    # times are epoch-millisecond UTC; startTime is inclusive, endTime exclusive.
    y, mo = (int(x) for x in month.split("-"))
    month_start = datetime(y, mo, 1, tzinfo=timezone.utc)
    month_end = (datetime(y + 1, 1, 1, tzinfo=timezone.utc) if mo == 12
                 else datetime(y, mo + 1, 1, tzinfo=timezone.utc))
    job_start = datetime.strptime(params["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    job_end = (datetime.strptime(params["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
               + timedelta(days=1))
    win_start = max(month_start, job_start)
    win_end = min(month_end, job_end)
    if win_start >= win_end:
        return ("skipped", "")

    cursor = int(win_start.timestamp() * 1000)
    end_ms = int(win_end.timestamp() * 1000)
    rows = []
    sess = _session()
    while cursor < end_ms:
        q = {"symbol": sym, "interval": interval, "startTime": cursor,
             "endTime": end_ms - 1, "limit": 1000}
        try:
            resp = sess.get(BINANCE_KLINES_URL, params=q, timeout=config.REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as e:
            raise RetryableError(f"network error: {e}") from e

        if resp.status_code in (429, 418):  # rate limited / IP banned — back off
            raise RetryableError(f"rate limited ({resp.status_code})")
        if resp.status_code >= 500:
            raise RetryableError(f"server error {resp.status_code}")
        if resp.status_code != 200:
            # 400 = bad symbol/interval: a permanent error, not worth retrying.
            raise ValueError(f"binance {resp.status_code}: {resp.text[:120]}")

        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        last_open = batch[-1][0]
        cursor = last_open + 1  # advance past the last candle returned
        if len(batch) < 1000:
            break

    if not rows:
        # No klines in this window (symbol not yet listed) — nothing to retry.
        return ("skipped", "")

    os.makedirs(out_dir, exist_ok=True)
    tmp = out_path + ".part"
    with open(tmp, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["open_time", "open", "high", "low", "close", "volume",
                        "close_time", "trades"])
        for k in rows:
            open_dt = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
            close_dt = datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc)
            writer.writerow([
                open_dt.isoformat(sep=" ", timespec="milliseconds"),
                k[1], k[2], k[3], k[4], k[5],
                close_dt.isoformat(sep=" ", timespec="milliseconds"),
                k[8],
            ])
    os.replace(tmp, out_path)  # atomic: a partial write never looks complete
    return ("done", out_path)


# Registry: job type -> (planner, runner, human label).
JOB_TYPES = {
    "dukascopy": (dukascopy_plan, dukascopy_run, "Dukascopy bi5 download"),
    "decode": (decode_plan, decode_run, "Decode bi5 -> CSV"),
    "yfinance": (yfinance_plan, yfinance_run, "yfinance prices + metadata"),
    "binance": (binance_plan, binance_run, "Binance crypto OHLCV"),
}

# SEC EDGAR and FRED job types live in their own modules (each has its own
# rate limiter / API specifics). Imported last so they can reuse RetryableError
# from this module without a circular-import problem, then merged in above.
from . import sec  # noqa: E402
from . import fred  # noqa: E402
from . import cftc  # noqa: E402

JOB_TYPES.update(sec.SEC_JOB_TYPES)
JOB_TYPES.update(fred.FRED_JOB_TYPES)
JOB_TYPES.update(cftc.CFTC_JOB_TYPES)
