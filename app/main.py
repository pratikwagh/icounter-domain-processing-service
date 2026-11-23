"""Application module for main functionality."""


import asyncio
from typing import Any
import logging
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, status

from app.database import Database
from app.schemas import JobCreate, JobCreated
from app.worker import JobWorker, normalize_domain


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def create_app(
    db_path: str = "jobs.db",
    start_workers: bool = True,
    retention_days: int | None = None,
    queue_maxsize: int = 1000,
) -> Any:
    """Create app."""
    db = Database(db_path)

    # Read retention from the environment unless explicitly supplied.
    if retention_days is None:
        retention_days = int(
            os.getenv("JOB_RETENTION_DAYS", "10")
        )

    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")

    if queue_maxsize < 1:
        raise ValueError("queue_maxsize must be greater than zero")

    # Bounded in-memory queue for job IDs.
    queue = asyncio.Queue(maxsize=queue_maxsize)

    # Prevent concurrent API requests from both passing the queue-capacity
    # check before either request inserts its job.
    job_submission_lock = asyncio.Lock()

    worker_tasks = []
    cleanup_task = None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> Any:
        """Lifespan."""
        nonlocal cleanup_task

        await db.initialize()

        # Delete expired terminal jobs during startup.
        deleted = await db.cleanup_expired_jobs(retention_days)

        logger.info(
            "Startup cleanup deleted %s expired jobs",
            deleted,
        )

        # Recover jobs interrupted by a previous shutdown.
        pending_job_ids = await db.recover_interrupted_jobs()

        if start_workers:
            # Constructor order matches JobWorker(queue, database).
            worker = JobWorker(queue, db)

            # Start two background worker coroutines.
            for worker_number in range(2):
                task = asyncio.create_task(
                    worker.run_forever(),
                    name=f"domain-worker-{worker_number + 1}",
                )
                worker_tasks.append(task)

            # Re-enqueue jobs recovered from the database.
            for job_id in pending_job_ids:
                await queue.put(job_id)

        async def cleanup_loop() -> Any:
            """Cleanup loop."""
            while True:
                await asyncio.sleep(24 * 60 * 60)

                try:
                    deleted_count = await db.cleanup_expired_jobs(
                        retention_days
                    )

                    logger.info(
                        "Scheduled cleanup deleted %s expired jobs",
                        deleted_count,
                    )

                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Scheduled job cleanup failed"
                    )

        cleanup_task = asyncio.create_task(
            cleanup_loop(),
            name="job-cleanup",
        )

        logger.info(
            "Application started. Job retention: %s days",
            retention_days,
        )

        try:
            yield

        finally:
            # Stop periodic cleanup.
            if cleanup_task is not None:
                cleanup_task.cancel()

                try:
                    await cleanup_task
                except asyncio.CancelledError:
                    pass

            # Stop background workers.
            for task in worker_tasks:
                task.cancel()

            if worker_tasks:
                await asyncio.gather(
                    *worker_tasks,
                    return_exceptions=True,
                )

            worker_tasks.clear()

            logger.info("Application shutdown complete")

    app = FastAPI(
        title="Domain Processing Service",
        description=(
            "Asynchronously processes domains and retrieves "
            "DNS and HTTP information."
        ),
        lifespan=lifespan,
    )

    # Expose these for health reporting and application-level tests.
    app.state.database = db
    app.state.job_queue = queue

    @app.get("/")
    async def root() -> Any:
        """Root."""
        return {
            "message": "Domain Processing Service is running",
            "docs": "/docs",
            "redoc": "/redoc",
            "health": "/health",
        }

    @app.get("/health")
    async def health() -> Any:
        """Health."""
        return {
            "status": "ok",
            "workers_enabled": start_workers,
            "queue_size": queue.qsize(),
        }

    @app.post(
        "/jobs",
        response_model=JobCreated,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_job(request: JobCreate) -> Any:
        # Normalize domain names and remove duplicates.
        # Invalid non-empty domain strings are intentionally retained.
        """Create job."""
        domains = list(
            dict.fromkeys(
                normalize_domain(domain)
                for domain in request.domains
            )
        )

        # Empty input is normally rejected by Pydantic.
        # This is an additional defensive check.
        if not domains:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="At least one domain must be provided",
            )

        job_id = str(uuid.uuid4())

        try:
            # Queue capacity check, persistence, and enqueueing are protected
            # as one admission operation. The lock prevents two concurrent
            # requests from both claiming the final queue slot.
            async with job_submission_lock:
                if queue.full():
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="Job queue is full. Please retry later.",
                    )

                # Persist first. There is no await between the successful
                # commit and put_nowait, and other producers are behind the
                # lock, so queue admission cannot race with another request.
                await db.create_job(job_id, domains)

                if start_workers:
                    queue.put_nowait(job_id)

        except HTTPException:
            raise

        except Exception:
            logger.exception(
                "Failed to create job %s",
                job_id,
            )

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Could not create job",
            )

        logger.info(
            "Created job %s with %s domains",
            job_id,
            len(domains),
        )

        return JobCreated(
            job_id=job_id,
            status="pending",
            total_domains=len(domains),
        )

    @app.get("/jobs/{job_id}")
    async def get_job(
        job_id: str,
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> Any:
        """Get job."""
        result = await db.get_job(
            job_id=job_id,
            limit=limit,
            offset=offset,
        )

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found",
            )

        return result

    return app


app = create_app()
