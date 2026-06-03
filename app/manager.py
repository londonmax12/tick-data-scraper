"""The download manager engine.

An asyncio scheduler runs in the background of the FastAPI process:

* A poll loop watches the ``jobs`` table and starts a worker coroutine for any
  job in the ``queued`` state.
* Each worker enumerates its job's unfinished tasks and runs them through a
  global thread-pool (the download/decode code is blocking I/O), bounded by a
  shared semaphore so the whole manager never exceeds ``MAX_CONCURRENCY``.
* Transient task failures are retried automatically with exponential backoff.
  A task that exhausts ``MAX_ATTEMPTS`` is parked as permanently failed but the
  rest of the job keeps going.
* Pause flips the job to ``paused``; in-flight tasks drain and no new ones
  start. Resume re-queues it and the worker picks up the remaining tasks.
* On process startup, jobs that were ``running`` are reset to ``queued`` so an
  interrupted run resumes from its first unfinished task.
"""
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from . import config, db, updates
from .jobs import JOB_TYPES, RetryableError


class Manager:
    def __init__(self, database: db.Database):
        self.db = database
        self._sem = asyncio.Semaphore(config.MAX_CONCURRENCY)
        self._pool = ThreadPoolExecutor(max_workers=config.MAX_CONCURRENCY)
        self._workers: dict[str, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # ---- lifecycle ------------------------------------------------------

    def start(self):
        self.db.recover_on_startup()
        self._loop_task = asyncio.create_task(self._poll_loop())

    async def shutdown(self):
        self._stop.set()
        if self._loop_task:
            self._loop_task.cancel()
        for w in list(self._workers.values()):
            w.cancel()
        self._pool.shutdown(wait=False, cancel_futures=True)

    async def _poll_loop(self):
        """Pick up queued jobs and reap finished workers."""
        while not self._stop.is_set():
            try:
                for job in self.db.list_jobs():
                    jid = job["id"]
                    if job["status"] == db.QUEUED and jid not in self._workers:
                        self._workers[jid] = asyncio.create_task(self._run_job(jid))
                # Reap completed worker tasks.
                for jid in [j for j, t in self._workers.items() if t.done()]:
                    self._workers.pop(jid, None)
                self._run_due_schedules()
            except Exception:  # noqa: BLE001 - poll loop must never die
                pass
            await asyncio.sleep(1.0)

    def _run_due_schedules(self):
        """Fire any auto-update schedule that's come due, then re-arm it."""
        now = time.time()
        for sch in self.db.due_schedules(now):
            try:
                updates.enqueue_update(self.db, sch["rel"])
            except Exception:  # noqa: BLE001 - a bad/empty dataset must not wedge the loop
                pass
            # Re-arm regardless of outcome so a failing dataset isn't retried
            # every second.
            self.db.mark_schedule_ran(sch["id"], sch["cadence"], now)

    # ---- per-job worker -------------------------------------------------

    async def _run_job(self, job_id: str):
        job = self.db.get_job(job_id)
        if not job:
            return
        planner, runner, _ = JOB_TYPES[job["type"]]
        params = job["params"]

        self.db.set_job_status(job_id, db.RUNNING)

        # Enumerate tasks once (idempotent — INSERT OR IGNORE keeps progress).
        try:
            self.db.add_tasks(job_id, planner(params))
        except Exception as e:  # noqa: BLE001 - planning failure fails the job
            self.db.set_job_status(job_id, db.FAILED, error=f"planning failed: {e}")
            return
        self.db.refresh_job_counts(job_id)

        pending = self.db.get_unfinished_tasks(job_id)
        inflight: set[asyncio.Task] = set()
        idx = 0

        while True:
            status = self.db.get_job_status(job_id)
            if status in (db.PAUSED, db.CANCELLED):
                for t in inflight:
                    t.cancel()
                return

            # Fill the in-flight set up to the concurrency limit.
            while idx < len(pending) and len(inflight) < config.MAX_CONCURRENCY:
                task_row = pending[idx]
                idx += 1
                inflight.add(asyncio.create_task(
                    self._run_task(job_id, runner, params, task_row)))

            if not inflight and idx >= len(pending):
                # Drained the current batch. Anything left to retry?
                pending = self.db.get_unfinished_tasks(job_id)
                idx = 0
                if not pending:
                    break  # nothing more to do -> finalize below
                continue

            done, inflight = await asyncio.wait(
                inflight, timeout=1.0, return_when=asyncio.FIRST_COMPLETED)
            if done:
                self.db.refresh_job_counts(job_id)

        # Finalize.
        self.db.refresh_job_counts(job_id)
        if self.db.has_permanent_failures(job_id):
            self.db.set_job_status(
                job_id, db.FAILED,
                error="some tasks failed after max retries (use Retry to re-run)")
        else:
            self.db.set_job_status(job_id, db.COMPLETED)
            self._maybe_chain_decode(job)

    async def _run_task(self, job_id: str, runner, params: dict, task_row: dict):
        """Run one task in the thread pool with auto-retry/backoff."""
        attempts = task_row["attempts"]
        key = task_row["key"]
        task_id = task_row["id"]
        loop = asyncio.get_running_loop()

        while True:
            # Stop retrying if the job was paused/cancelled between attempts.
            status = self.db.get_job_status(job_id)
            if status in (db.PAUSED, db.CANCELLED):
                return

            attempts += 1
            try:
                async with self._sem:
                    result_status, output = await loop.run_in_executor(
                        self._pool, runner, params, key)
                self.db.mark_task(task_id, result_status, attempts=attempts,
                                error=None, output=output or None)
                return
            except asyncio.CancelledError:
                raise
            except RetryableError as e:
                if attempts >= config.MAX_ATTEMPTS:
                    self.db.mark_task(task_id, db.T_FAILED, attempts=attempts,
                                    error=str(e))
                    return
                self.db.mark_task(task_id, db.T_FAILED, attempts=attempts,
                                error=str(e))
                backoff = min(config.RETRY_BACKOFF_MAX,
                            config.RETRY_BACKOFF_BASE ** attempts)
                await asyncio.sleep(backoff)
                # loop and retry
            except Exception as e:  # noqa: BLE001 - unexpected: park as failed
                self.db.mark_task(task_id, db.T_FAILED,
                                attempts=config.MAX_ATTEMPTS, error=repr(e))
                return

    def _maybe_chain_decode(self, job: dict):
        """If a Dukascopy job requested it, queue a decode job for the ticker."""
        if job["type"] == "dukascopy" and job["params"].get("decode_after"):
            ticker = job["params"]["ticker"]
            self.db.create_job(
                "decode", f"Decode {ticker}", {"ticker": ticker})


# Module-level singletons, wired up by main.py on startup.
DB: db.Database | None = None
MANAGER: Manager | None = None
