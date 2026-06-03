"""FastAPI app: REST API for the queue + static web UI."""
import csv as _csv
import os
import tempfile
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from . import config, db, manager, updates
from .jobs import JOB_TYPES
from . import cftc, futures  # after .jobs: leaf source modules load via jobs (avoids a cycle)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

# The on-disk data roots the Library is allowed to read from. The first path
# component of every Library `rel` must be one of these keys, and the resolved
# absolute path must stay inside the matching root (see _safe_resolve).
LIBRARY_SOURCES = [
    ("results", "Dukascopy ticks (results/)", config.RESULTS_DIR),
    ("results_csv", "Decoded CSVs (results_csv/)", config.CSV_DIR),
    ("company_data", "yfinance (company_data/)", config.COMPANY_DIR),
    ("crypto_data", "Binance crypto (crypto_data/)", config.CRYPTO_DIR),
    ("crypto_futures_data", "Binance futures (crypto_futures_data/)", config.FUTURES_DIR),
    ("sec_data", "SEC EDGAR by ticker (sec_data/)", config.SEC_DIR),
    ("sec_13f", "SEC 13F holdings (sec_13f/)", config.SEC_13F_DIR),
    ("fred_data", "FRED macro series (fred_data/)", config.FRED_DIR),
    ("cftc_data", "CFTC Commitments of Traders (cftc_data/)", config.CFTC_DIR),
]
LIBRARY_ROOTS = {key: root for key, _, root in LIBRARY_SOURCES}


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager.DB = db.Database()
    manager.MANAGER = Manager = manager.Manager(manager.DB)
    Manager.start()
    yield
    await Manager.shutdown()


app = FastAPI(title="Quant Data Download Manager", lifespan=lifespan)


# ---- request models -----------------------------------------------------

class DukascopyJob(BaseModel):
    ticker: str = Field(..., examples=["GBPJPY"])
    start_date: str = Field(..., examples=["2023-01-01"])  # YYYY-MM-DD
    end_date: str = Field(..., examples=["2023-01-31"])
    hour_min: int = 0
    hour_max: int = 23
    decode_after: bool = False


class DecodeJob(BaseModel):
    ticker: str


class YFinanceJob(BaseModel):
    tickers: list[str] = Field(..., examples=[["AAPL", "MSFT"]])
    interval: str = "1d"
    start_date: str | None = None  # YYYY-MM-DD; blank = full history (period=max)
    end_date: str | None = None


class BinanceJob(BaseModel):
    symbols: list[str] = Field(..., examples=[["BTCUSDT", "ETHUSDT"]])
    interval: str = "1d"
    start_date: str = Field(..., examples=["2023-01-01"])  # YYYY-MM-DD
    end_date: str = Field(..., examples=["2023-12-31"])


class BinanceFuturesJob(BaseModel):
    market: str = "usdm"  # usdm (USDT/USDC-margined) | coinm (coin-margined)
    symbols: list[str] = Field(..., examples=[["BTCUSDT", "ETHUSDT"]])
    start_date: str = Field(..., examples=["2023-01-01"])  # YYYY-MM-DD
    end_date: str = Field(..., examples=["2023-12-31"])
    # Price-series klines: which series, at what bar size. Empty kinds = none.
    kinds: list[str] = Field(default_factory=lambda: ["ohlcv"],
                             examples=[["ohlcv", "mark", "index", "premium"]])
    interval: str = "1h"
    # Funding-rate history (one funding.csv per symbol).
    funding: bool = True
    # /futures/data sentiment metrics (rolling ~30 days) at a period bucket.
    metrics: list[str] = Field(default_factory=list,
                               examples=[["open_interest", "top_accounts", "taker"]])
    period: str = "5m"
    contract_type: str = "PERPETUAL"  # COIN-M only: PERPETUAL | CURRENT_QUARTER | …


class SecFactsJob(BaseModel):
    tickers: list[str] = Field(..., examples=[["AAPL", "MSFT"]])
    # Which SEC products to pull for these tickers. Each maps to its own job type.
    facts: bool = True          # XBRL fundamentals
    submissions: bool = True    # full filing index + company profile
    insider: bool = False       # Form 3/4/5 insider transactions (many filings)
    start_date: str | None = None  # filing-date filter for insider, YYYY-MM-DD
    end_date: str | None = None


class Sec13FJob(BaseModel):
    ciks: list[str] = Field(..., examples=[["0001067983"]])  # institution CIKs
    start_date: str | None = None  # filing-date filter, YYYY-MM-DD
    end_date: str | None = None


class FredJob(BaseModel):
    # At least one of series_ids / release_id / category_id / search_text.
    series_ids: list[str] = Field(default_factory=list, examples=[["UNRATE", "GDP"]])
    release_id: str | None = None    # expand a FRED release into its series
    category_id: str | None = None   # expand a FRED category into its series
    search_text: str | None = None   # expand a full-text search into its series
    max_series: int = 1000           # cap on a discovery fan-out
    # Observation transform (server-side).
    units: str = "lin"               # lin|chg|ch1|pch|pc1|pca|cch|cca|log
    frequency: str | None = None     # d|w|bw|m|q|sa|a (re-aggregate); blank = native
    aggregation_method: str = "avg"  # avg|sum|eop (used when frequency is set)
    observation_start: str | None = None  # YYYY-MM-DD
    observation_end: str | None = None
    # ALFRED point-in-time (vintages); leave blank for latest revised data.
    realtime_start: str | None = None
    realtime_end: str | None = None
    vintage_dates: list[str] = Field(default_factory=list)


class CftcJob(BaseModel):
    # Which Commitments-of-Traders report families to pull; one job per family.
    reports: list[str] = Field(default_factory=lambda: ["legacy_fut"],
                               examples=[["legacy_fut", "disagg_fut", "tff_fut"]])
    # At least one of contract_codes / commodity_group / commodity_subgroup /
    # market_filter / all_markets must select markets.
    contract_codes: list[str] = Field(default_factory=list, examples=[["13874A", "001602"]])
    commodity_group: str | None = None     # e.g. "FINANCIAL INSTRUMENTS"
    commodity_subgroup: str | None = None   # e.g. "CURRENCY"
    market_filter: str | None = None        # substring of the market/exchange name
    all_markets: bool = False               # pull every market in the report
    max_markets: int = 1000                 # cap on a discovery fan-out
    start_date: str | None = None  # report-date filter, YYYY-MM-DD
    end_date: str | None = None


# ---- helpers ------------------------------------------------------------

def _db() -> db.Database:
    assert manager.DB is not None
    return manager.DB


def _progress(job: dict) -> dict:
    total = job["total"] or 0
    done = job["done"] or 0
    pct = round(100 * done / total, 1) if total else 0.0
    job["percent"] = pct
    return job


# ---- API ----------------------------------------------------------------

@app.get("/api/jobs")
def list_jobs():
    return [_progress(j) for j in _db().list_jobs()]


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = _db().get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return _progress(job)


@app.post("/api/jobs/dukascopy")
def create_dukascopy(body: DukascopyJob):
    title = (f"{body.ticker} {body.start_date}→{body.end_date} "
             f"({body.hour_min:02d}-{body.hour_max:02d}h)")
    jid = _db().create_job("dukascopy", title, body.model_dump())
    return {"id": jid}


@app.post("/api/jobs/decode")
def create_decode(body: DecodeJob):
    jid = _db().create_job("decode", f"Decode {body.ticker}", body.model_dump())
    return {"id": jid}


@app.post("/api/jobs/yfinance")
def create_yfinance(body: YFinanceJob):
    n = len(body.tickers)
    title = f"yfinance ({n} tickers, {body.interval})"
    jid = _db().create_job("yfinance", title, body.model_dump())
    return {"id": jid}


@app.post("/api/jobs/binance")
def create_binance(body: BinanceJob):
    n = len(body.symbols)
    title = (f"Binance ({n} symbols, {body.interval}) "
             f"{body.start_date}→{body.end_date}")
    jid = _db().create_job("binance", title, body.model_dump())
    return {"id": jid}


@app.post("/api/jobs/binance_futures")
def create_binance_futures(body: BinanceFuturesJob):
    """Queue one job per selected futures product (klines / funding / metrics)."""
    if body.market not in futures.MARKETS:
        raise HTTPException(400, f"unknown market '{body.market}'")
    if not body.symbols:
        raise HTTPException(400, "select at least one symbol")
    kinds = [k for k in body.kinds if k in futures.KLINE_KINDS]
    metrics = [m for m in body.metrics if m in futures.METRICS]
    if body.kinds and not kinds:
        raise HTTPException(400, "unknown kline kind(s) selected")
    if body.metrics and not metrics:
        raise HTTPException(400, "unknown metric(s) selected")
    if not (kinds or body.funding or metrics):
        raise HTTPException(400, "select klines, funding, or at least one metric")
    if metrics and body.period not in futures.METRIC_PERIODS:
        raise HTTPException(400, f"unknown metrics period '{body.period}'")

    n = len(body.symbols)
    tag = futures.MARKETS[body.market]["label"]
    common = body.model_dump()
    created = []
    if kinds:
        params = {**common, "kinds": kinds}
        title = (f"Binance futures klines [{tag}] ({n} symbols, "
                 f"{'/'.join(kinds)} {body.interval}) {body.start_date}→{body.end_date}")
        created.append({"type": "binance_futures",
                        "id": _db().create_job("binance_futures", title, params)})
    if body.funding:
        title = f"Binance funding [{tag}] ({n} symbols) {body.start_date}→{body.end_date}"
        created.append({"type": "binance_funding",
                        "id": _db().create_job("binance_funding", title, common)})
    if metrics:
        params = {**common, "metrics": metrics}
        title = f"Binance futures metrics [{tag}] ({n} symbols, {'/'.join(metrics)} {body.period})"
        created.append({"type": "binance_metrics",
                        "id": _db().create_job("binance_metrics", title, params)})
    return {"jobs": created}


@app.post("/api/jobs/sec")
def create_sec(body: SecFactsJob):
    """Queue one job per selected SEC product for a batch of tickers."""
    n = len(body.tickers)
    common = {"tickers": body.tickers,
              "start_date": body.start_date, "end_date": body.end_date}
    created = []
    if body.facts:
        created.append({"type": "sec_facts", "id": _db().create_job(
            "sec_facts", f"SEC fundamentals ({n} tickers)", common)})
    if body.submissions:
        created.append({"type": "sec_submissions", "id": _db().create_job(
            "sec_submissions", f"SEC filings index ({n} tickers)", common)})
    if body.insider:
        created.append({"type": "sec_insider", "id": _db().create_job(
            "sec_insider", f"SEC insider trades ({n} tickers)", common)})
    if not created:
        raise HTTPException(400, "select at least one SEC product")
    return {"jobs": created}


@app.post("/api/jobs/sec_13f")
def create_sec_13f(body: Sec13FJob):
    n = len(body.ciks)
    jid = _db().create_job("sec_13f", f"SEC 13F holdings ({n} institutions)",
                           body.model_dump())
    return {"id": jid}


@app.post("/api/jobs/fred")
def create_fred(body: FredJob):
    if not (body.series_ids or body.release_id or body.category_id
            or body.search_text):
        raise HTTPException(400, "provide series ids, a release/category id, or "
                                 "search text")
    # Describe the job by whatever drives it (explicit ids vs. a discovery).
    if body.series_ids:
        what = f"{len(body.series_ids)} series"
    elif body.search_text:
        what = f"search '{body.search_text}'"
    elif body.release_id:
        what = f"release {body.release_id}"
    else:
        what = f"category {body.category_id}"
    suffix = body.units + (f"/{body.frequency}" if body.frequency else "")
    jid = _db().create_job("fred", f"FRED {what} ({suffix})", body.model_dump())
    return {"id": jid}


@app.post("/api/jobs/cftc")
def create_cftc(body: CftcJob):
    """Queue one Commitments-of-Traders job per selected report family."""
    reports = [r.strip().lower() for r in body.reports if r.strip()]
    bad = [r for r in reports if r not in cftc.REPORTS]
    if bad:
        raise HTTPException(400, f"unknown CFTC report(s): {', '.join(bad)}")
    if not reports:
        raise HTTPException(400, "select at least one CFTC report")
    if not (body.contract_codes or body.commodity_group or body.commodity_subgroup
            or body.market_filter or body.all_markets):
        raise HTTPException(400, "select markets: contract codes, a commodity "
                                 "group/subgroup, a market filter, or all markets")
    # Describe the markets by whatever drives the selection.
    if body.contract_codes:
        what = f"{len(body.contract_codes)} markets"
    elif body.all_markets:
        what = "all markets"
    elif body.market_filter:
        what = f"filter '{body.market_filter}'"
    else:
        what = (body.commodity_subgroup or body.commodity_group or "markets")

    created = []
    for report in reports:
        params = {**body.model_dump(), "report": report}
        title = f"CFTC {cftc.REPORTS[report][1]} — {what}"
        created.append({"type": "cftc", "report": report,
                        "id": _db().create_job("cftc", title, params)})
    return {"jobs": created}


@app.post("/api/jobs/{job_id}/pause")
def pause_job(job_id: str):
    job = _db().get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job["status"] in (db.QUEUED, db.RUNNING):
        _db().set_job_status(job_id, db.PAUSED)
    return {"ok": True}


@app.post("/api/jobs/{job_id}/resume")
def resume_job(job_id: str):
    job = _db().get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job["status"] in (db.PAUSED, db.FAILED, db.CANCELLED):
        # Re-arm permanently-failed tasks so a resume actually retries them.
        if job["status"] == db.FAILED:
            _db().reset_failed_tasks(job_id)
        _db().set_job_status(job_id, db.QUEUED, error=None)
    return {"ok": True}


@app.post("/api/jobs/{job_id}/retry")
def retry_job(job_id: str):
    job = _db().get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    _db().reset_failed_tasks(job_id)
    _db().set_job_status(job_id, db.QUEUED, error=None)
    return {"ok": True}


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    job = _db().get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    _db().set_job_status(job_id, db.CANCELLED)
    return {"ok": True}


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str):
    # Flag cancelled first so any running worker stops touching it.
    if _db().get_job(job_id):
        _db().set_job_status(job_id, db.CANCELLED)
    _db().delete_job(job_id)
    return {"ok": True}


@app.get("/api/config")
def get_config():
    return {
        "concurrency": config.MAX_CONCURRENCY,
        "max_attempts": config.MAX_ATTEMPTS,
        "job_types": {k: v[2] for k, v in JOB_TYPES.items()},
    }


# ---- library: browse / preview / download on-disk data ------------------

def _safe_resolve(rel: str) -> str:
    """Resolve a Library `rel` path to an absolute path, refusing anything that
    escapes its declared data root (path traversal / symlink-out)."""
    rel = (rel or "").replace("\\", "/").strip("/")
    parts = [p for p in rel.split("/") if p not in ("", ".", "..")]
    if not parts or parts[0] not in LIBRARY_ROOTS:
        raise HTTPException(400, "invalid path")
    root = os.path.realpath(LIBRARY_ROOTS[parts[0]])
    target = os.path.realpath(os.path.join(config.BASE_DIR, *parts))
    if os.path.commonpath([root, target]) != root:
        raise HTTPException(400, "path escapes data directory")
    return target


def _dir_stats(d: str) -> tuple[int, int]:
    """(file count, total bytes) under a directory, ignoring temp .part files."""
    files = total = 0
    for dirpath, _, names in os.walk(d):
        for n in names:
            if n.endswith(".part"):
                continue
            files += 1
            try:
                total += os.path.getsize(os.path.join(dirpath, n))
            except OSError:
                pass
    return files, total


@app.get("/api/library")
def library():
    """List downloaded datasets (one per ticker/symbol) grouped by source."""
    sources = []
    for key, label, root in LIBRARY_SOURCES:
        datasets = []
        if os.path.isdir(root):
            for name in sorted(os.listdir(root)):
                path = os.path.join(root, name)
                if not os.path.isdir(path):
                    continue
                files, total = _dir_stats(path)
                datasets.append({"name": name, "files": files, "bytes": total,
                                 "rel": f"{key}/{name}"})
        sources.append({"key": key, "label": label, "datasets": datasets})
    return {"sources": sources}


@app.get("/api/library/files")
def library_files(rel: str):
    """The previewable (.csv) files inside one dataset directory."""
    base = _safe_resolve(rel)
    if not os.path.isdir(base):
        raise HTTPException(404, "dataset not found")
    out = []
    for dirpath, _, names in os.walk(base):
        for n in sorted(names):
            if not n.endswith(".csv"):
                continue
            full = os.path.join(dirpath, n)
            rel_root = os.path.relpath(full, config.BASE_DIR).replace("\\", "/")
            out.append({
                "rel": rel_root,
                "name": os.path.relpath(full, base).replace("\\", "/"),
                "bytes": os.path.getsize(full),
            })
    out.sort(key=lambda f: f["name"])
    return {"files": out}


@app.get("/api/library/preview")
def library_preview(rel: str, limit: int = 200):
    """First `limit` rows of a CSV as columns + rows for a table view."""
    path = _safe_resolve(rel)
    if not os.path.isfile(path) or not path.endswith(".csv"):
        raise HTTPException(400, "not a csv file")
    limit = max(1, min(limit, 1000))
    rows = []
    with open(path, newline="") as f:
        reader = _csv.reader(f)
        header = next(reader, [])
        for i, r in enumerate(reader):
            if i >= limit:
                break
            rows.append(r)
    return {"columns": header, "rows": rows}


# Preferred y-columns for the quick chart, best first (price-like across all
# our CSV shapes: yfinance prices, crypto klines, decoded ticks).
_CHART_COLUMNS = ["close", "Close", "bid", "ask", "Open", "open", "value",
                  "open_interest_all", "sumOpenInterest", "longShortRatio",
                  "buySellRatio", "funding_rate"]
_CHART_SCAN_CAP = 500_000  # rows read before we stop (then downsample)


@app.get("/api/library/series")
def library_series(rel: str, max_points: int = 600):
    """A downsampled single-column series for a sparkline-style chart."""
    path = _safe_resolve(rel)
    if not os.path.isfile(path) or not path.endswith(".csv"):
        raise HTTPException(400, "not a csv file")
    max_points = max(10, min(max_points, 3000))
    with open(path, newline="") as f:
        reader = _csv.reader(f)
        header = next(reader, [])
        yi = next((header.index(c) for c in _CHART_COLUMNS if c in header), None)
        if yi is None:
            yi = 1 if len(header) > 1 else 0
        xs, ys = [], []
        for n, r in enumerate(reader):
            if n >= _CHART_SCAN_CAP:
                break
            if yi >= len(r):
                continue
            try:
                y = float(r[yi])
            except ValueError:
                continue
            xs.append(r[0] if r else "")
            ys.append(y)
    total = len(ys)
    if total > max_points:
        stride = (total + max_points - 1) // max_points
        xs, ys = xs[::stride], ys[::stride]
    return {"x": xs, "y": ys, "column": header[yi] if yi < len(header) else "",
            "points": len(ys), "total": total}


@app.get("/api/library/download")
def library_download(rel: str):
    """Download a single file as an attachment."""
    path = _safe_resolve(rel)
    if not os.path.isfile(path):
        raise HTTPException(404, "file not found")
    return FileResponse(path, filename=os.path.basename(path),
                        media_type="application/octet-stream")


@app.get("/api/library/zip")
def library_zip(rel: str):
    """Zip a whole dataset directory and stream it back."""
    base = _safe_resolve(rel)
    if not os.path.isdir(base):
        raise HTTPException(404, "dataset not found")
    tmp = tempfile.NamedTemporaryFile(prefix="qd_", suffix=".zip", delete=False)
    tmp.close()
    parent = os.path.dirname(base)
    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, _, names in os.walk(base):
            for n in names:
                if n.endswith(".part"):
                    continue
                full = os.path.join(dirpath, n)
                zf.write(full, os.path.relpath(full, parent))
    return FileResponse(tmp.name, filename=os.path.basename(base) + ".zip",
                        media_type="application/zip",
                        background=BackgroundTask(os.remove, tmp.name))


# ---- coverage: which hours are downloaded, which are holes --------------

class FillBody(BaseModel):
    start: str | None = None
    end: str | None = None
    hour_min: int = 0
    hour_max: int = 23


def _disk_present(ticker: str) -> set[tuple[str, int]]:
    """Set of (YYYY-MM-DD, hour) hour-files actually on disk for a ticker."""
    root = os.path.join(config.RESULTS_DIR, ticker)
    present: set[tuple[str, int]] = set()
    if not os.path.isdir(root):
        return present
    for folder in os.listdir(root):
        fp = os.path.join(root, folder)
        if not os.path.isdir(fp):
            continue
        try:
            day = datetime.strptime(folder, "%Y_%m_%d")
        except ValueError:
            continue
        for fn in os.listdir(fp):
            stem = fn[:-4] if fn.endswith(".bi5") else None
            if not stem or not stem.endswith("h"):
                continue
            try:
                hour = int(stem[:-1])
            except ValueError:
                continue
            try:
                if os.path.getsize(os.path.join(fp, fn)) > 0:
                    present.add((day.strftime("%Y-%m-%d"), hour))
            except OSError:
                pass
    return present


def _compute_coverage(ticker: str, start: str | None, end: str | None,
                      hmin: int, hmax: int) -> dict:
    """Classify every expected (day, hour) cell as present / closed / hole.

    * present — file on disk, or a task recorded ``done``
    * closed  — legitimately empty: Dukascopy 404 (``skipped``) or a Saturday
                (FX is shut worldwide all Saturday UTC), so not a real gap
    * hole    — anything else: failed, or never attempted and not on disk
    """
    hmin = max(0, min(hmin, 23))
    hmax = max(hmin, min(hmax, 23))
    present_disk = _disk_present(ticker)
    db_status = _db().dukascopy_task_status(ticker)

    # Infer the range from whatever we know about if not given.
    if not start or not end:
        seen = {d for d, _ in present_disk}
        seen.update(k.rsplit("/", 1)[0].replace("/", "-") for k in db_status)
        seen = sorted(seen)
        if not seen:
            return {"ticker": ticker, "start": None, "end": None,
                    "hour_min": hmin, "hour_max": hmax, "present": 0,
                    "closed": 0, "holes": 0, "coverage_pct": 0.0,
                    "days": [], "hole_keys": []}
        start = start or seen[0]
        end = end or seen[-1]

    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    present = closed = holes = 0
    days, hole_keys = [], []
    day = s
    while day <= e:
        date_str = day.strftime("%Y-%m-%d")
        is_saturday = day.weekday() == 5
        cells = []
        for hour in range(hmin, hmax + 1):
            key = f"{day.strftime('%Y/%m/%d')}/{hour:02d}"
            st = db_status.get(key)
            if (date_str, hour) in present_disk or st == db.T_DONE:
                cells.append("p")
                present += 1
            elif st == db.T_SKIPPED or is_saturday:
                cells.append("c")
                closed += 1
            else:
                cells.append("h")
                holes += 1
                hole_keys.append(key)
        days.append({"date": date_str, "cells": "".join(cells)})
        day += timedelta(days=1)

    denom = present + holes
    return {
        "ticker": ticker, "start": start, "end": end,
        "hour_min": hmin, "hour_max": hmax,
        "present": present, "closed": closed, "holes": holes,
        "coverage_pct": round(100 * present / denom, 1) if denom else 100.0,
        "days": days, "hole_keys": hole_keys,
    }


def _check_ticker(ticker: str) -> str:
    t = ticker.strip().upper()
    if not t or not t.isalnum():  # also blocks path separators / traversal
        raise HTTPException(400, "invalid ticker")
    return t


@app.get("/api/coverage/{ticker}")
def coverage(ticker: str, start: str | None = None, end: str | None = None,
             hour_min: int = 0, hour_max: int = 23):
    return _compute_coverage(_check_ticker(ticker), start, end, hour_min, hour_max)


@app.post("/api/coverage/{ticker}/fill")
def coverage_fill(ticker: str, body: FillBody):
    ticker = _check_ticker(ticker)
    cov = _compute_coverage(ticker, body.start, body.end, body.hour_min, body.hour_max)
    holes = cov["hole_keys"]
    if not holes:
        return {"created": False, "holes": 0}
    params = {
        "ticker": ticker,
        "start_date": cov["start"], "end_date": cov["end"],
        "hour_min": cov["hour_min"], "hour_max": cov["hour_max"],
        "only_keys": holes,
    }
    jid = _db().create_job("dukascopy", f"Fill gaps {ticker} ({len(holes)} hours)", params)
    return {"created": True, "holes": len(holes), "id": jid}


# ---- incremental updates + auto-update schedules ------------------------

class UpdateBody(BaseModel):
    rel: str


class ScheduleBody(BaseModel):
    rel: str
    cadence: str = "daily"  # hourly | daily | weekly


@app.post("/api/library/update")
def library_update(body: UpdateBody):
    """Queue the job(s) that bring one dataset up to today."""
    try:
        return updates.enqueue_update(_db(), body.rel)
    except updates.UpdateError as e:
        raise HTTPException(400, str(e))


@app.get("/api/schedules")
def list_schedules():
    return _db().list_schedules()


@app.post("/api/schedules")
def create_schedule(body: ScheduleBody):
    try:
        sid = _db().create_schedule(body.rel, body.cadence)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"id": sid}


@app.post("/api/schedules/{sid}/toggle")
def toggle_schedule(sid: str):
    _db().toggle_schedule(sid)
    return {"ok": True}


@app.delete("/api/schedules/{sid}")
def delete_schedule(sid: str):
    _db().delete_schedule(sid)
    return {"ok": True}


# ---- static UI ----------------------------------------------------------

@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
