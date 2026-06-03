# Quant Data — Download Manager

A web app that turns the original scripts (`scrape.py`, `process.py`,
`tickers/process_tickers.py`) into a queueable, **auto-resuming** download
manager. Queue jobs in the browser, watch live progress, pause/resume, and let
dropped or interrupted downloads retry and resume on their own — including
after the app is restarted.

![queue UI](app/static/index.html)

## What it does

Four job types, all queued from the same UI:

| Job type    | Source                        | Output                    | Resumable unit   |
|-------------|-------------------------------|---------------------------|------------------|
| **Dukascopy** | Dukascopy FX tick feed      | `results/<ticker>/`       | one hour `.bi5`  |
| **Decode**    | local `.bi5` → CSV          | `results_csv/<ticker>/`   | one day CSV      |
| **yfinance**  | Yahoo Finance               | `company_data/<ticker>/`  | one ticker       |
| **Binance**   | Binance public klines API   | `crypto_data/<symbol>/`   | one symbol-month |

* **yfinance** saves the actual **OHLCV price history** (`<ticker>_prices_<interval>.csv`)
  plus company info, dividends and splits. Pick a bar interval (1d … 1m) and an
  optional date range — leave the dates blank for full history.
* **Binance** pulls crypto **OHLCV klines** (no API key) at any interval, one CSV
  per symbol-month so a long range resumes file-by-file. Rate limits (HTTP 429)
  retry automatically.

### Library

A **Library** tab browses what's already on disk: every dataset grouped by
source with its file count and size. Click **Preview** to see the first rows of
any CSV plus a quick close-price chart, **Download** a single file, or grab a
whole dataset as a **zip**. All file access is confined to the four output
directories — paths that try to escape are rejected.

### Coverage & gaps

For a Dukascopy ticker, **Coverage** (in the Library) shows a day × hour heat-grid
classifying every expected hour as **have** (on disk), **closed** (legitimately
empty — Dukascopy 404 `skipped`, or a Saturday when FX is shut), or **missing**
(a real hole). **Fill gaps** queues a Dukascopy job that re-fetches *only* the
missing hours; market-closed ones simply 404 → `skipped`, so coverage becomes
authoritative after one pass.

### Incremental updates

Every dataset in the Library has an **Update** button that inspects what's on
disk and queues a job to carry it forward to today — from the last downloaded
day (Dukascopy), the last month (Binance, re-fetching the partial latest month),
or a full refresh (yfinance). **⏱ Auto** schedules that update on an `hourly` /
`daily` / `weekly` cadence; the manager's poll loop fires due schedules in the
background and survives restarts (schedule state lives in `manager.db`).

## Run it

```powershell
pip install -r requirements.txt
python run.py
```

Then open <http://127.0.0.1:8000>. Other host/port:

```powershell
python run.py --host 0.0.0.0 --port 9000
```

## How auto-resume works

State lives in a SQLite database (`manager.db`) split into **jobs** and the
individual **tasks** inside them. A task is only ever run if its status is not
already `done`/`skipped`, so nothing is re-downloaded needlessly.

* **Dropped / paused connections** — a network error, timeout, or 5xx marks the
  task failed and it is retried automatically with exponential backoff (up to
  `DM_MAX_ATTEMPTS` times). Market-closed hours (HTTP 404) are `skipped`, not
  retried.
* **Manual pause / resume** — Pause flips the job to `paused`; in-flight tasks
  finish and no new ones start. Resume re-queues it and the remaining tasks
  pick up where they stopped.
* **App restart** — on startup, any job that was `running` is reset to
  `queued`, and because per-task progress is persisted the job resumes from its
  first unfinished task. Files are written atomically (`.part` → rename) so a
  half-written file is never mistaken for a complete one.

## Configuration (environment variables)

| Var               | Default | Meaning                                  |
|-------------------|---------|------------------------------------------|
| `DM_CONCURRENCY`  | `12`    | Max download tasks running at once       |
| `DM_TIMEOUT`      | `30`    | Per-request network timeout (seconds)    |
| `DM_MAX_ATTEMPTS` | `50`    | Retries before a task is permanently failed |

## Layout

```
app/
  config.py    settings + Dukascopy headers
  db.py        SQLite queue (jobs + tasks, startup recovery)
  jobs.py      the three job types: plan() + run() per task
  manager.py   asyncio scheduler: worker pool, retry/backoff, pause/resume
  main.py      FastAPI REST API + serves the UI
  static/index.html   single-page web UI (live polling)
run.py         launches uvicorn
```

The standalone `scrape.py` / `process.py` / `tickers/process_tickers.py` scripts
still run on their own; the app reimplements their logic sliced per-task for
resumability.
