"""Application module for database functionality."""


import json
from typing import Any
from contextlib import asynccontextmanager

import aiosqlite


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    total_domains INTEGER NOT NULL,
    completed_domains INTEGER NOT NULL DEFAULT 0,
    failed_domains INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS domain_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    domain TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    ip_addresses TEXT,
    dns_records TEXT,
    http_status INTEGER,
    page_title TEXT,
    response_time_ms REAL,
    error TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, domain)
);

CREATE INDEX IF NOT EXISTS idx_domain_results_job_status
ON domain_results(job_id, status, id);
"""


TERMINAL_JOB_STATUSES = (
    "completed",
    "partially_completed",
    "completed_with_errors",
    "failed",
)


class Database:

    def __init__(self, path: str) -> None:
        """Initialize the instance."""
        self.path = path

    @asynccontextmanager
    async def connection(self) -> Any:
        """Create and automatically close an asynchronous SQLite connection."""

        async with aiosqlite.connect(
            self.path,
            timeout=30,
        ) as db:

            db.row_factory = aiosqlite.Row

            # Apply these settings to every connection.
            await db.execute("PRAGMA foreign_keys = ON")
            await db.execute("PRAGMA busy_timeout = 30000")

            yield db

    async def initialize(self) -> Any:
        """Create database tables and indexes."""

        async with self.connection() as db:
            await db.executescript(SCHEMA)
            await db.commit()

    async def create_job(
        self,
        job_id: str,
        domains: list[str],
    ) -> Any:
        """Create a job and its associated domain records."""

        async with self.connection() as db:

            await db.execute(
                """
                INSERT INTO jobs (
                    job_id,
                    status,
                    total_domains
                )
                VALUES (?, 'pending', ?)
                """,
                (job_id, len(domains)),
            )

            await db.executemany(
                """
                INSERT INTO domain_results (
                    job_id,
                    domain
                )
                VALUES (?, ?)
                """,
                [
                    (job_id, domain)
                    for domain in domains
                ],
            )

            await db.commit()

    async def update_job_status(
        self,
        job_id: str,
        status: str,
    ) -> Any:
        """Update the overall job status."""

        async with self.connection() as db:

            await db.execute(
                """
                UPDATE jobs
                SET status = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """,
                (status, job_id),
            )

            await db.commit()

    async def get_pending_domains(
        self,
        job_id: str,
        limit: int = 100,
    ) -> Any:
        """Fetch pending domains for a job in batches."""

        async with self.connection() as db:

            cursor = await db.execute(
                """
                SELECT id, domain
                FROM domain_results
                WHERE job_id = ?
                  AND status = 'pending'
                ORDER BY id
                LIMIT ?
                """,
                (job_id, limit),
            )

            rows = await cursor.fetchall()

            return [
                dict(row)
                for row in rows
            ]

    async def mark_processing(
        self,
        result_id: int,
    ) -> Any:
        """Mark an individual domain as processing."""

        async with self.connection() as db:

            await db.execute(
                """
                UPDATE domain_results
                SET status = 'processing',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (result_id,),
            )

            await db.commit()

    async def save_domain_result(
        self,
        result_id: int,
        result: dict,
    ) -> Any:
        """Persist the DNS and HTTP processing result."""

        async with self.connection() as db:

            await db.execute(
                """
                UPDATE domain_results
                SET status = ?,
                    ip_addresses = ?,
                    dns_records = ?,
                    http_status = ?,
                    page_title = ?,
                    response_time_ms = ?,
                    error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    result["status"],
                    json.dumps(
                        result.get("ip_addresses", [])
                    ),
                    json.dumps(
                        result.get("dns_records", {})
                    ),
                    result.get("http_status"),
                    result.get("page_title"),
                    result.get("response_time_ms"),
                    result.get("error"),
                    result_id,
                ),
            )

            await db.commit()

    async def refresh_job_counts(
        self,
        job_id: str,
    ) -> Any:
        """
        Recalculate job counters and determine its overall status.

        Possible final statuses:
        - completed: all domains succeeded
        - partially_completed: some succeeded and some failed
        - failed: all domains failed
        - in_progress: at least one domain is still pending/processing
        """

        async with self.connection() as db:

            cursor = await db.execute(
                """
                SELECT
                    COUNT(*) AS total,

                    SUM(
                        CASE
                            WHEN status = 'completed'
                            THEN 1
                            ELSE 0
                        END
                    ) AS completed,

                    SUM(
                        CASE
                            WHEN status = 'failed'
                            THEN 1
                            ELSE 0
                        END
                    ) AS failed,

                    SUM(
                        CASE
                            WHEN status IN ('pending', 'processing')
                            THEN 1
                            ELSE 0
                        END
                    ) AS remaining

                FROM domain_results
                WHERE job_id = ?
                """,
                (job_id,),
            )

            row = await cursor.fetchone()
            counts = dict(row)

            total = counts["total"] or 0
            completed = counts["completed"] or 0
            failed = counts["failed"] or 0
            remaining = counts["remaining"] or 0

            if remaining > 0:
                job_status = "in_progress"

            elif completed > 0 and failed > 0:
                job_status = "partially_completed"

            elif failed > 0:
                job_status = "failed"

            else:
                job_status = "completed"

            await db.execute(
                """
                UPDATE jobs
                SET status = ?,
                    total_domains = ?,
                    completed_domains = ?,
                    failed_domains = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
                """,
                (
                    job_status,
                    total,
                    completed,
                    failed,
                    job_id,
                ),
            )

            await db.commit()

            return job_status

    async def get_job(
        self,
        job_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Any:
        """Fetch job metadata and paginated domain results."""

        async with self.connection() as db:

            cursor = await db.execute(
                """
                SELECT *
                FROM jobs
                WHERE job_id = ?
                """,
                (job_id,),
            )

            job = await cursor.fetchone()

            if job is None:
                return None

            cursor = await db.execute(
                """
                SELECT *
                FROM domain_results
                WHERE job_id = ?
                ORDER BY id
                LIMIT ? OFFSET ?
                """,
                (job_id, limit, offset),
            )

            rows = await cursor.fetchall()

            results = []

            for row in rows:

                item = dict(row)

                item["ip_addresses"] = json.loads(
                    item["ip_addresses"] or "[]"
                )

                item["dns_records"] = json.loads(
                    item["dns_records"] or "{}"
                )

                results.append(item)

            return {
                "job": dict(job),
                "results": results,
            }

    async def cleanup_expired_jobs(
        self,
        retention_days: int = 10,
    ) -> int:
        """
        Delete terminal jobs that have not been updated within the retention period.

        Pending and in-progress jobs are not deleted.

        Associated domain results are automatically deleted through
        the ON DELETE CASCADE foreign key constraint.
        """

        if retention_days < 1:
            raise ValueError(
                "retention_days must be at least 1"
            )

        async with self.connection() as db:

            placeholders = ", ".join(
                "?" for _ in TERMINAL_JOB_STATUSES
            )

            query = f"""
                DELETE FROM jobs
                WHERE status IN ({placeholders})
                  AND updated_at < datetime('now', ?)
            """

            cursor = await db.execute(
                query,
                (
                    *TERMINAL_JOB_STATUSES,
                    f"-{retention_days} days",
                ),
            )

            await db.commit()

            return cursor.rowcount

    async def recover_interrupted_jobs(self) -> Any:
        """
        Recover jobs interrupted by application shutdown.

        Processing domain records are reset to pending.
        Interrupted jobs are reset to pending and returned
        for re-enqueuing by the application.
        """

        async with self.connection() as db:

            # Recover domains that were being processed.
            await db.execute(
                """
                UPDATE domain_results
                SET status = 'pending',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = 'processing'
                """
            )

            # Recover jobs interrupted during execution.
            await db.execute(
                """
                UPDATE jobs
                SET status = 'pending',
                    updated_at = CURRENT_TIMESTAMP
                WHERE status IN (
                    'in_progress',
                    'pending'
                )
                """
            )

            cursor = await db.execute(
                """
                SELECT job_id
                FROM jobs
                WHERE status = 'pending'
                ORDER BY created_at
                """
            )

            rows = await cursor.fetchall()

            job_ids = [
                row["job_id"]
                for row in rows
            ]

            await db.commit()

            return job_ids
