"""Application module for worker functionality."""


import asyncio
from typing import Any
import logging
import os
import re

from app.services.dns_service import lookup_dns
from app.services.http_service import fetch_page


logger = logging.getLogger(__name__)

DOMAIN_TIMEOUT_SECONDS = float(
    os.getenv("DOMAIN_TIMEOUT_SECONDS", "300")
)

DOMAIN_RE = re.compile(
    r"^(?=.{1,253}$)"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z]{2,63}$",
    re.IGNORECASE,
)


def normalize_domain(value: str) -> str:
    """
    Normalize a hostname.

    Invalid values are retained so the worker can save an
    individual failed result instead of rejecting the entire job.
    """
    raw = value.strip().rstrip(".").lower()

    if (
        not raw
        or "://" in raw
        or "/" in raw
        or ":" in raw
        or " " in raw
    ):
        return raw

    try:
        return raw.encode("idna").decode("ascii")
    except UnicodeError:
        return raw


def is_valid_domain(domain: str) -> bool:
    """Is valid domain."""
    return bool(DOMAIN_RE.fullmatch(domain))


async def inspect_domain(domain: str) -> dict:
    """
    Perform DNS and HTTP inspection for one domain.

    DNS failure does not prevent HTTP inspection. If HTTP succeeds,
    the result is completed with a DNS error warning.
    """
    if not is_valid_domain(domain):
        return {
            "status": "failed",
            "error": "Invalid domain format",
        }

    try:
        ips, dns_records = await lookup_dns(domain)
        dns_error = None

    except Exception as exc:
        logger.warning(
            "DNS lookup failed for %s: %s",
            domain,
            exc,
        )
        ips, dns_records = [], {}
        dns_error = (
            f"DNS lookup failed: {type(exc).__name__}: {exc}"
        )

    try:
        web = await fetch_page(domain)

        return {
            "status": "completed",
            "ip_addresses": ips,
            "dns_records": dns_records,
            **web,
            "error": dns_error,
        }

    except Exception as exc:
        logger.warning(
            "HTTP inspection failed for %s: %s",
            domain,
            exc,
        )

        return {
            "status": "failed",
            "ip_addresses": ips,
            "dns_records": dns_records,
            "error": f"{type(exc).__name__}: {exc}",
        }


class JobWorker:
    def __init__(
        self,
        queue: asyncio.Queue,
        database,
        batch_size: int = 50,
        concurrency: int = 20,
    ) -> None:
        # Keep the argument order consistent with main.py:
        # JobWorker(queue, db)
        """Initialize the instance."""
        self.queue = queue
        self.database = database
        self.batch_size = batch_size
        self.concurrency = concurrency

    async def run_forever(self) -> Any:
        """Run forever."""
        while True:
            job_id = await self.queue.get()

            try:
                await self.process_job(job_id)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Unexpected failure while processing job %s",
                    job_id,
                )
                await self.database.update_job_status(
                    job_id,
                    "failed",
                )

            finally:
                self.queue.task_done()

    async def process_job(self, job_id: str) -> Any:
        """Process job."""
        logger.info("Starting job %s", job_id)

        await self.database.update_job_status(
            job_id,
            "in_progress",
        )

        semaphore = asyncio.Semaphore(self.concurrency)

        async def process_one(row) -> Any:
            """Process one."""
            domain_id = row["id"]
            domain = row["domain"]

            await self.database.mark_processing(domain_id)

            async with semaphore:
                try:
                    # A timeout is applied independently to each domain.
                    async with asyncio.timeout(
                        DOMAIN_TIMEOUT_SECONDS
                    ):
                        result = await inspect_domain(domain)

                except TimeoutError:
                    logger.warning(
                        "Processing timed out for domain %s",
                        domain,
                    )

                    result = {
                        "status": "failed",
                        "error": "Processing timed out",
                    }

                except asyncio.CancelledError:
                    raise

                except Exception as exc:
                    logger.exception(
                        "Unexpected error processing domain %s",
                        domain,
                    )

                    result = {
                        "status": "failed",
                        "error": (
                            f"{type(exc).__name__}: {exc}"
                        ),
                    }

                await self.database.save_domain_result(
                    domain_id,
                    result,
                )

        while True:
            batch = await self.database.get_pending_domains(
                job_id,
                self.batch_size,
            )

            if not batch:
                break

            # Only batch_size tasks are created at a time.
            # The semaphore limits active domain inspections.
            await asyncio.gather(
                *(process_one(row) for row in batch)
            )

        await self.database.refresh_job_counts(job_id)

        logger.info("Finished processing job %s", job_id)
