"""SQLite-backed persistence for the download queue.

State is split into two tables:

* ``jobs``  — one row per queued download job (a ticker date-range, a decode
              run, a yfinance batch). Holds aggregate progress counters and the
              control status the UI flips (queued / running / paused / ...).
* ``tasks`` — the individual resumable units of work inside a job (one .bi5
              hour file, one date folder to decode, one ticker to fetch). This
              granularity is what makes auto-resume work: only tasks whose
              status is not ``done``/``skipped`` are ever re-run, so an
              interrupted job picks up exactly where it left off.

All writes go through a single lock because download tasks touch the DB from a
thread pool. The connection uses WAL so reads from the API thread never block.
"""
import json
import sqlite3
import threading
import time
import uuid
from typing import Any, Optional

from . import config

# Job lifecycle statuses.
QUEUED = "queued"
RUNNING = "running"
PAUSED = "paused"
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"

# Task statuses.
T_PENDING = "pending"
T_DONE = "done"
T_FAILED = "failed"
T_SKIPPED = "skipped"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    title       TEXT NOT NULL,
    params      TEXT NOT NULL,
    status      TEXT NOT NULL,
    total       INTEGER NOT NULL DEFAULT 0,
    done        INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    key         TEXT NOT NULL,
    label       TEXT NOT NULL,
    status      TEXT NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    output      TEXT,
    updated_at  REAL NOT NULL,
    UNIQUE(job_id, key),
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tasks_job ON tasks(job_id, status);
"""


class Database:
    def __init__(self, path: str = config.DB_PATH):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # ---- jobs -----------------------------------------------------------

    def create_job(self, type_: str, title: str, params: dict) -> str:
        job_id = uuid.uuid4().hex
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO jobs (id, type, title, params, status, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (job_id, type_, title, json.dumps(params), QUEUED, now, now),
            )
            self._conn.commit()
        return job_id

    def set_job_status(self, job_id: str, status: str, error: Optional[str] = None):
        with self._lock:
            if error is None:
                self._conn.execute(
                    "UPDATE jobs SET status=?, updated_at=? WHERE id=?",
                    (status, time.time(), job_id),
                )
            else:
                self._conn.execute(
                    "UPDATE jobs SET status=?, error=?, updated_at=? WHERE id=?",
                    (status, error, time.time(), job_id),
                )
            self._conn.commit()

    def get_job_status(self, job_id: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
        return row["status"] if row else None

    def refresh_job_counts(self, job_id: str):
        """Recompute the aggregate progress counters from the tasks table."""
        with self._lock:
            row = self._conn.execute(
                "SELECT "
                " COUNT(*) AS total, "
                " SUM(CASE WHEN status IN ('done','skipped') THEN 1 ELSE 0 END) AS done, "
                " SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed "
                "FROM tasks WHERE job_id=?",
                (job_id,),
            ).fetchone()
            self._conn.execute(
                "UPDATE jobs SET total=?, done=?, failed=?, updated_at=? WHERE id=?",
                (row["total"] or 0, row["done"] or 0, row["failed"] or 0,
                 time.time(), job_id),
            )
            self._conn.commit()

    def get_job(self, job_id: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
        return self._job_to_dict(row) if row else None

    def list_jobs(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC"
            ).fetchall()
        return [self._job_to_dict(r) for r in rows]

    def delete_job(self, job_id: str):
        with self._lock:
            self._conn.execute("DELETE FROM tasks WHERE job_id=?", (job_id,))
            self._conn.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            self._conn.commit()

    @staticmethod
    def _job_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["params"] = json.loads(d["params"])
        return d

    # ---- tasks ----------------------------------------------------------

    def add_tasks(self, job_id: str, tasks: list[tuple[str, str]]):
        """Bulk-insert (key, label) task rows, ignoring ones already present.

        Idempotent: re-running for the same job keeps existing task state, so
        a resumed job does not lose progress.
        """
        now = time.time()
        with self._lock:
            self._conn.executemany(
                "INSERT OR IGNORE INTO tasks (job_id, key, label, status, updated_at)"
                " VALUES (?,?,?,?,?)",
                [(job_id, key, label, T_PENDING, now) for key, label in tasks],
            )
            self._conn.commit()

    def count_tasks(self, job_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM tasks WHERE job_id=?", (job_id,)
            ).fetchone()
        return row["c"]

    def get_unfinished_tasks(self, job_id: str) -> list[dict]:
        """Tasks still needing work: pending or transiently failed (retryable)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE job_id=? AND status IN (?,?) "
                "AND attempts < ? ORDER BY id",
                (job_id, T_PENDING, T_FAILED, config.MAX_ATTEMPTS),
            ).fetchall()
        return [dict(r) for r in rows]

    def has_permanent_failures(self, job_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM tasks WHERE job_id=? AND status=? AND attempts >= ?",
                (job_id, T_FAILED, config.MAX_ATTEMPTS),
            ).fetchone()
        return row["c"] > 0

    def mark_task(self, task_id: int, status: str, *, attempts: Optional[int] = None,
                  error: Optional[str] = None, output: Optional[str] = None):
        with self._lock:
            fields = ["status=?", "updated_at=?"]
            values: list[Any] = [status, time.time()]
            if attempts is not None:
                fields.append("attempts=?")
                values.append(attempts)
            fields.append("error=?")
            values.append(error)
            if output is not None:
                fields.append("output=?")
                values.append(output)
            values.append(task_id)
            self._conn.execute(
                f"UPDATE tasks SET {', '.join(fields)} WHERE id=?", values
            )
            self._conn.commit()

    def reset_failed_tasks(self, job_id: str):
        """Re-arm permanently-failed tasks for another round (manual retry)."""
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET status=?, attempts=0, error=NULL, updated_at=? "
                "WHERE job_id=? AND status=?",
                (T_PENDING, time.time(), job_id, T_FAILED),
            )
            self._conn.commit()

    # ---- startup recovery ----------------------------------------------

    def recover_on_startup(self):
        """Any job that was mid-flight when the process died goes back to the
        queue so the manager re-picks it and resumes its unfinished tasks."""
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE status=?",
                (QUEUED, time.time(), RUNNING),
            )
            self._conn.commit()
