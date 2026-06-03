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

def yfinance_plan(params: dict) -> list[tuple[str, str]]:
    tickers = params.get("tickers") or []
    seen = set()
    tasks = []
    for t in tickers:
        t = str(t).strip().upper()
        if t and t.isalpha() and t not in seen:
            seen.add(t)
            tasks.append((t, t))
    return tasks


def yfinance_run(params: dict, key: str) -> tuple[str, str]:
    import yfinance as yf  # imported lazily; heavy module

    ticker = key
    out_dir = os.path.join(config.COMPANY_DIR, ticker)
    info_path = os.path.join(out_dir, f"{ticker}_company_info.csv")
    if os.path.exists(info_path):
        return ("done", info_path)

    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period="max")
    except Exception as e:  # noqa: BLE001 - yfinance raises many error types
        raise RetryableError(f"yfinance error: {e}") from e

    if hist is None or hist.empty:
        return ("skipped", "")

    import pandas as pd

    first_date = hist.index[0]
    try:
        name = tk.info.get("longName", ticker)
    except Exception:  # noqa: BLE001 - .info can fail independently of history
        name = ticker
    divs = tk.dividends
    splits = tk.splits

    os.makedirs(out_dir, exist_ok=True)
    pd.DataFrame([{"Ticker": ticker, "Name": name, "First Date": first_date}]).to_csv(
        info_path, index=False)

    div_rows, last = [], None
    for date in divs.index:
        cur = divs[date]
        if cur != last:
            div_rows.append({"Date": date, "Dividend": cur})
            last = cur
    if div_rows:
        pd.DataFrame(div_rows).to_csv(
            os.path.join(out_dir, f"{ticker}_dividends.csv"), index=False)

    split_rows, last = [], None
    for date in splits.index:
        cur = splits[date]
        if cur != last:
            split_rows.append({"Date": date, "Stock Split": cur})
            last = cur
    if split_rows:
        pd.DataFrame(split_rows).to_csv(
            os.path.join(out_dir, f"{ticker}_stock_splits.csv"), index=False)

    return ("done", info_path)


# Registry: job type -> (planner, runner, human label).
JOB_TYPES = {
    "dukascopy": (dukascopy_plan, dukascopy_run, "Dukascopy bi5 download"),
    "decode": (decode_plan, decode_run, "Decode bi5 -> CSV"),
    "yfinance": (yfinance_plan, yfinance_run, "yfinance metadata"),
}
