"""Build "update to latest" jobs for an already-downloaded dataset.

Given a Library dataset ``rel`` ('results/GBPJPY', 'crypto_data/BTCUSDT',
'company_data/AAPL', 'results_csv/GBPJPY'), inspect what is on disk and queue
the download jobs that carry it forward to today. Shared by the REST endpoint
(:mod:`app.main`) and the background scheduler (:mod:`app.manager`) so a manual
"Update" and an automatic one behave identically.
"""
import os
from datetime import datetime, timezone

from . import config

_ROOTS = {
    "results": config.RESULTS_DIR,
    "results_csv": config.CSV_DIR,
    "company_data": config.COMPANY_DIR,
    "crypto_data": config.CRYPTO_DIR,
    "sec_data": config.SEC_DIR,
    "sec_13f": config.SEC_13F_DIR,
}


class UpdateError(ValueError):
    """The dataset can't be updated (unknown source, bad path, nothing on disk)."""


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _dataset_dir(rel: str) -> tuple[str, str, str]:
    """Validate ``rel`` ('<source>/<name>') and return (source, name, abs_dir).

    Same containment guarantee as the Library API: the resolved directory must
    stay inside its declared data root, so a crafted ``rel`` can't make the
    updater delete files elsewhere.
    """
    rel = (rel or "").replace("\\", "/").strip("/")
    parts = [p for p in rel.split("/") if p not in ("", ".", "..")]
    if len(parts) != 2 or parts[0] not in _ROOTS:
        raise UpdateError("invalid dataset")
    source, name = parts
    root = os.path.realpath(_ROOTS[source])
    target = os.path.realpath(os.path.join(root, name))
    if os.path.commonpath([root, target]) != root:
        raise UpdateError("path escapes data directory")
    if not os.path.isdir(target):
        raise UpdateError("dataset not found")
    return source, name, target


def enqueue_update(database, rel: str) -> dict:
    """Queue the job(s) that bring dataset ``rel`` up to today; return their ids."""
    source, name, d = _dataset_dir(rel)
    if source == "results":
        return _update_dukascopy(database, name, d)
    if source == "crypto_data":
        return _update_binance(database, name, d)
    if source == "company_data":
        return _update_yfinance(database, name, d)
    if source == "results_csv":
        return _update_decode(database, name)
    if source == "sec_data":
        return _update_sec(database, name, d)
    if source == "sec_13f":
        return _update_sec_13f(database, name)
    raise UpdateError("unsupported dataset")


def _update_dukascopy(database, ticker: str, d: str) -> dict:
    days = []
    for folder in os.listdir(d):
        if os.path.isdir(os.path.join(d, folder)):
            try:
                days.append(datetime.strptime(folder, "%Y_%m_%d"))
            except ValueError:
                pass
    if not days:
        raise UpdateError("no day folders to extend")
    # Re-fetch from the last present day (its already-downloaded hours skip via
    # the on-disk check, so only genuinely new hours hit the network).
    last = max(days).strftime("%Y-%m-%d")
    today = _today()
    params = {"ticker": ticker, "start_date": last, "end_date": today,
              "hour_min": 0, "hour_max": 23}
    title = f"Update {ticker} {last}→{today}"
    return {"jobs": [{"id": database.create_job("dukascopy", title, params),
                      "title": title}]}


def _update_binance(database, symbol: str, d: str) -> dict:
    today = _today()
    jobs = []
    for interval in sorted(os.listdir(d)):
        idir = os.path.join(d, interval)
        if not os.path.isdir(idir):
            continue
        months = []
        for fn in os.listdir(idir):
            if fn.endswith(".csv"):
                try:
                    months.append(datetime.strptime(fn[:-4], "%Y-%m"))
                except ValueError:
                    pass
        if not months:
            continue
        last = max(months)
        # The newest month file is likely partial (downloaded mid-month); drop
        # it so the update re-downloads that month complete plus everything new.
        try:
            os.remove(os.path.join(idir, last.strftime("%Y-%m") + ".csv"))
        except OSError:
            pass
        params = {"symbols": [symbol], "interval": interval,
                  "start_date": last.strftime("%Y-%m-01"), "end_date": today}
        title = f"Update {symbol} {interval} →{today}"
        jobs.append({"id": database.create_job("binance", title, params),
                     "title": title})
    if not jobs:
        raise UpdateError("no crypto months to extend")
    return {"jobs": jobs}


def _update_yfinance(database, ticker: str, d: str) -> dict:
    prefix, suffix = f"{ticker}_prices_", ".csv"
    intervals = sorted({fn[len(prefix):-len(suffix)] for fn in os.listdir(d)
                        if fn.startswith(prefix) and fn.endswith(suffix)})
    if not intervals:
        raise UpdateError("no price files to refresh")
    jobs = []
    for interval in intervals:
        # yfinance with no date range pulls full history; delete the existing
        # file so the run actually re-downloads (latest bars included).
        try:
            os.remove(os.path.join(d, f"{ticker}_prices_{interval}.csv"))
        except OSError:
            pass
        params = {"tickers": [ticker], "interval": interval}
        title = f"Update {ticker} {interval}"
        jobs.append({"id": database.create_job("yfinance", title, params),
                     "title": title})
    return {"jobs": jobs}


def _update_decode(database, ticker: str) -> dict:
    title = f"Re-decode {ticker}"
    return {"jobs": [{"id": database.create_job("decode", title, {"ticker": ticker}),
                      "title": title}]}


def _update_sec(database, ticker: str, d: str) -> dict:
    """Refresh whichever SEC products already exist for a ticker.

    Point-in-time products (facts, filing index) are deleted so they re-download
    fresh. Insider trades are append-only per-filing, so re-running just pulls
    newly filed forms — existing per-accession files skip on the on-disk check.
    """
    jobs = []
    common = {"tickers": [ticker]}
    files = set(os.listdir(d))

    if f"{ticker}_facts.csv" in files:
        for fn in (f"{ticker}_facts.csv", f"{ticker}_concepts.csv",
                   f"{ticker}_entity.csv"):
            try:
                os.remove(os.path.join(d, fn))
            except OSError:
                pass
        jobs.append({"id": database.create_job(
            "sec_facts", f"Update SEC fundamentals {ticker}", common),
            "title": f"Update SEC fundamentals {ticker}"})

    if f"{ticker}_filings.csv" in files:
        for fn in (f"{ticker}_filings.csv", f"{ticker}_profile.csv"):
            try:
                os.remove(os.path.join(d, fn))
            except OSError:
                pass
        jobs.append({"id": database.create_job(
            "sec_submissions", f"Update SEC filings {ticker}", common),
            "title": f"Update SEC filings {ticker}"})

    if os.path.isdir(os.path.join(d, "insider")):
        jobs.append({"id": database.create_job(
            "sec_insider", f"Update SEC insider {ticker}", common),
            "title": f"Update SEC insider {ticker}"})

    if not jobs:
        raise UpdateError("no SEC products to update for this ticker")
    return {"jobs": jobs}


def _update_sec_13f(database, cik: str) -> dict:
    # New 13F filings download; already-saved periods skip on the on-disk check.
    title = f"Update SEC 13F {cik}"
    return {"jobs": [{"id": database.create_job(
        "sec_13f", title, {"ciks": [cik]}), "title": title}]}
