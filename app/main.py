"""FastAPI app: REST API for the queue + static web UI."""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import config, db, manager
from .jobs import JOB_TYPES

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


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
    jid = _db().create_job("yfinance", f"yfinance ({n} tickers)", body.model_dump())
    return {"id": jid}


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


# ---- static UI ----------------------------------------------------------

@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
