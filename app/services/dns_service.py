"""Application module for dns service functionality."""

import asyncio
from typing import Any
import logging

import dns.asyncresolver
import dns.exception
import dns.resolver


logger = logging.getLogger(__name__)

RECORD_TYPES = ("A", "TXT", "CNAME", "MX", "NS")
DNS_MAX_ATTEMPTS = 2  # Initial attempt + one retry.
DNS_RETRY_DELAY_SECONDS = 0.5


def is_retryable_dns_error(exc: Exception) -> bool:
    """Return True only for errors that may be transient."""
    return isinstance(
        exc,
        (
            dns.exception.Timeout,
            dns.resolver.NoNameservers,
            OSError,
        ),
    )


async def lookup_dns(
    domain: str,
    timeout: float = 3.0,
) -> tuple[list[str], dict]:
    """Lookup dns."""
    resolver = dns.asyncresolver.Resolver()
    resolver.lifetime = timeout

    async def lookup(record_type: str) -> Any:
        """Lookup."""
        for attempt in range(1, DNS_MAX_ATTEMPTS + 1):
            try:
                answer = await resolver.resolve(
                    domain,
                    record_type,
                    lifetime=timeout,
                )
                return record_type, [
                    record.to_text() for record in answer
                ], None

            except Exception as exc:
                should_retry = (
                    attempt < DNS_MAX_ATTEMPTS
                    and is_retryable_dns_error(exc)
                )

                if should_retry:
                    logger.warning(
                        "Transient DNS error for %s record of %s "
                        "(attempt %s/%s): %s. Retrying.",
                        record_type,
                        domain,
                        attempt,
                        DNS_MAX_ATTEMPTS,
                        exc,
                    )
                    await asyncio.sleep(DNS_RETRY_DELAY_SECONDS)
                    continue

                # Missing record types are normal for many domains.
                # Keep the existing result shape and do not fail the job.
                logger.debug(
                    "DNS lookup for %s record of %s failed: %s",
                    record_type,
                    domain,
                    type(exc).__name__,
                )
                return record_type, [], type(exc).__name__

        return record_type, [], "DNS lookup failed"

    results = await asyncio.gather(
        *(lookup(kind) for kind in RECORD_TYPES)
    )

    records = {
        kind: values
        for kind, values, _ in results
    }

    ips = records.get("A", [])

    return ips, records
