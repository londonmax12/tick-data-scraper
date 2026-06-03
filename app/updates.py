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
    "crypto_futures_data": config.FUTURES_DIR,
    "sec_data": config.SEC_DIR,
    "sec_13f": config.SEC_13F_DIR,
    "fred_data": config.FRED_DIR,
    "cftc_data": config.CFTC_DIR,
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
    if source == "crypto_futures_data":
        return _update_futures(database, name, d)
    if source == "company_data":
        return _update_yfinance(database, name, d)
    if source == "results_csv":
        return _update_decode(database, name)
    if source == "sec_data":
        return _update_sec(database, name, d)
    if source == "sec_13f":
        return _update_sec_13f(database, name)
    if source == "fred_data":
        return _update_fred(database, name, d)
    if source == "cftc_data":
        return _update_cftc(database, name, d)
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


def _read_futures_market(d: str) -> str:
    """The market a futures dataset belongs to, from its ``_market.csv`` sidecar;
    defaults to USD-M if the marker is missing or unreadable."""
    import csv  # local: updates.py is otherwise csv-free
    try:
        with open(os.path.join(d, "_market.csv"), newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        if len(rows) >= 2 and rows[1]:
            return rows[1][0].strip().lower() or "usdm"
    except OSError:
        pass
    return "usdm"


def _update_futures(database, symbol: str, d: str) -> dict:
    """Carry every futures product already on disk for a symbol forward to today.

    Klines (each kind × interval) re-fetch from their last month — the partial
    latest month file is dropped first so it re-downloads complete, exactly like
    the spot Binance updater. Funding and the /futures/data sentiment metrics get
    a full refresh (small, revised in place): delete and re-queue the same request.
    The market is recovered from the ``_market.csv`` marker the runner wrote.
    """
    from . import futures  # local: leaf source module loads via jobs (avoids a cycle)
    today = _today()
    market = _read_futures_market(d)
    jobs = []

    # --- price-series klines: one job per (kind, interval) ---
    dir_to_kind = {spec["dir"]: kind for kind, spec in futures.KLINE_KINDS.items()}
    for kind_dir, kind in dir_to_kind.items():
        kd = os.path.join(d, kind_dir)
        if not os.path.isdir(kd):
            continue
        for interval in sorted(os.listdir(kd)):
            idir = os.path.join(kd, interval)
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
            try:
                os.remove(os.path.join(idir, last.strftime("%Y-%m") + ".csv"))
            except OSError:
                pass
            params = {"market": market, "symbols": [symbol], "kinds": [kind],
                      "interval": interval,
                      "start_date": last.strftime("%Y-%m-01"), "end_date": today}
            title = f"Update {symbol} {kind} {interval} →{today}"
            jobs.append({"id": database.create_job("binance_futures", title, params),
                         "title": title})

    # --- funding history: full refresh ---
    if os.path.isfile(os.path.join(d, "funding.csv")):
        try:
            os.remove(os.path.join(d, "funding.csv"))
        except OSError:
            pass
        params = {"market": market, "symbols": [symbol],
                  "start_date": None, "end_date": today}
        title = f"Update {symbol} funding →{today}"
        jobs.append({"id": database.create_job("binance_funding", title, params),
                     "title": title})

    # --- sentiment metrics: re-pull the rolling window, grouped by period ---
    mdir = os.path.join(d, "metrics")
    if os.path.isdir(mdir):
        by_period: dict[str, list[str]] = {}
        for fn in sorted(os.listdir(mdir)):
            if not fn.endswith(".csv"):
                continue
            # Filenames are "<metric>_<period>.csv"; metric names contain
            # underscores (open_interest), so split on the last one.
            metric, _, period = fn[:-len(".csv")].rpartition("_")
            if metric not in futures.METRICS or period not in futures.METRIC_PERIODS:
                continue
            try:
                os.remove(os.path.join(mdir, fn))
            except OSError:
                pass
            by_period.setdefault(period, []).append(metric)
        for period, metrics in by_period.items():
            params = {"market": market, "symbols": [symbol],
                      "metrics": metrics, "period": period}
            title = f"Update {symbol} metrics ({'/'.join(metrics)} {period})"
            jobs.append({"id": database.create_job("binance_metrics", title, params),
                         "title": title})

    if not jobs:
        raise UpdateError("no futures data to refresh for this symbol")
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


def _update_fred(database, series_id: str, d: str) -> dict:
    """Re-pull every (units, frequency) variant already on disk for a series.

    A series is small and gets revised in place, so the cleanest refresh is a
    full re-download: each observations file encodes its transform in the name
    (``<id>_<units>_<freq>.csv``), so we delete it and re-queue that exact
    request. The ``_meta.csv`` sidecar is left for the run to rewrite.
    """
    prefix = f"{series_id}_"
    jobs = []
    seen = set()
    for fn in os.listdir(d):
        if not fn.endswith(".csv") or not fn.startswith(prefix):
            continue
        if fn == f"{series_id}_meta.csv":
            continue
        tag = fn[len(prefix):-len(".csv")]      # "<units>_<freq>"
        if "_" not in tag or tag in seen:
            continue
        seen.add(tag)
        units, freq_tag = tag.split("_", 1)
        frequency = None if freq_tag == "orig" else freq_tag
        try:
            os.remove(os.path.join(d, fn))
        except OSError:
            pass
        params = {"series_ids": [series_id], "units": units,
                  "frequency": frequency, "aggregation_method": "avg"}
        title = f"Update FRED {series_id} ({tag.replace('_', '/')})"
        jobs.append({"id": database.create_job("fred", title, params),
                     "title": title})
    if not jobs:
        raise UpdateError("no FRED series files to refresh")
    return {"jobs": jobs}


def _update_cftc(database, code: str, d: str) -> dict:
    """Re-pull every report family already on disk for a contract market code.

    A market's weekly history is small and the CFTC revises figures in place, so
    the cleanest refresh is a full re-download: each file encodes its report
    family in the name (``<code>_<report>.csv``), so we delete it and re-queue
    that exact request. The ``_meta.csv`` sidecar is left for the run to rewrite.
    """
    from . import cftc  # local: leaf source module loads via jobs (avoids a cycle)
    prefix = f"{code}_"
    jobs = []
    for fn in os.listdir(d):
        if not fn.endswith(".csv") or not fn.startswith(prefix):
            continue
        report = fn[len(prefix):-len(".csv")]      # "<report>"
        if report == "meta" or report not in cftc.REPORTS:
            continue
        try:
            os.remove(os.path.join(d, fn))
        except OSError:
            pass
        params = {"report": report, "contract_codes": [code]}
        title = f"Update CFTC {code} ({cftc.REPORTS[report][1]})"
        jobs.append({"id": database.create_job("cftc", title, params),
                     "title": title})
    if not jobs:
        raise UpdateError("no CFTC report files to refresh")
    return {"jobs": jobs}


def _update_sec_13f(database, cik: str) -> dict:
    # New 13F filings download; already-saved periods skip on the on-disk check.
    title = f"Update SEC 13F {cik}"
    return {"jobs": [{"id": database.create_job(
        "sec_13f", title, {"ciks": [cik]}), "title": title}]}
