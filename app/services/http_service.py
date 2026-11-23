"""Application module for http service functionality."""

import asyncio
from typing import Any
import ipaddress
import logging
import os
import socket
import time

import httpx
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

HTTP_MAX_ATTEMPTS = 2  # Initial attempt + one retry.
HTTP_RETRY_DELAY_SECONDS = 0.5
HTTP_MAX_RESPONSE_BYTES = int(
    os.getenv("HTTP_MAX_RESPONSE_BYTES", str(2 * 1024 * 1024))
)

if HTTP_MAX_RESPONSE_BYTES < 1:
    raise ValueError("HTTP_MAX_RESPONSE_BYTES must be greater than zero")


class ResponseTooLargeError(ValueError):
    """Raised when an HTTP response exceeds the configured size limit."""


def is_public_ip(value: str) -> bool:
    """Is public ip."""
    try:
        address = ipaddress.ip_address(value)
        return address.is_global
    except ValueError:
        return False


async def validate_public_destination(domain: str) -> Any:
    """Basic SSRF guard: reject hostnames resolving to non-public IP addresses."""
    loop = asyncio.get_running_loop()

    infos = None

    for attempt in range(1, 3):
        try:
            infos = await loop.getaddrinfo(
                domain,
                None,
                type=socket.SOCK_STREAM,
            )
            break

        except socket.gaierror as exc:
            if attempt == 1:
                logger.warning(
                    "Transient hostname resolution error for %s. Retrying: %s",
                    domain,
                    exc,
                )
                await asyncio.sleep(HTTP_RETRY_DELAY_SECONDS)
                continue

            raise ValueError(
                "Domain could not be resolved to an IP address"
            ) from exc

    addresses = {
        item[4][0]
        for item in infos
    }

    if not addresses or any(
        not is_public_ip(address)
        for address in addresses
    ):
        raise ValueError(
            "Requests to private, loopback, or non-public IP addresses are blocked"
        )


async def _read_limited_html(
    response: httpx.Response,
    max_response_bytes: int,
) -> str | None:
    """Read an HTML response incrementally without exceeding the limit."""

    content_length = response.headers.get("content-length")

    if content_length:
        try:
            declared_size = int(content_length)
        except ValueError:
            declared_size = None

        if (
            declared_size is not None
            and declared_size > max_response_bytes
        ):
            raise ResponseTooLargeError(
                f"HTTP response exceeds the {max_response_bytes}-byte limit"
            )

    content_type = response.headers.get("content-type", "")

    if "html" not in content_type.lower():
        return None

    chunks = []
    total_bytes = 0

    async for chunk in response.aiter_bytes():
        total_bytes += len(chunk)

        if total_bytes > max_response_bytes:
            raise ResponseTooLargeError(
                f"HTTP response exceeds the {max_response_bytes}-byte limit"
            )

        chunks.append(chunk)

    encoding = response.encoding or "utf-8"
    html = b"".join(chunks).decode(
        encoding,
        errors="replace",
    )

    soup = BeautifulSoup(html, "html.parser")

    if soup.title:
        return soup.title.get_text(strip=True) or None

    return None


async def fetch_page(
    domain: str,
    timeout: float = 8.0,
    max_response_bytes: int = HTTP_MAX_RESPONSE_BYTES,
) -> dict:
    """
    Fetch a domain over HTTPS, falling back to HTTP.

    Transient network errors and HTTP 5xx responses are retried once.
    Response bodies are streamed and bounded by max_response_bytes.
    """
    if max_response_bytes < 1:
        raise ValueError("max_response_bytes must be greater than zero")

    await validate_public_destination(domain)

    headers = {
        "User-Agent": "DomainInspector/1.0",
        "Accept-Encoding": "identity",
    }

    started = time.perf_counter()
    last_error = None

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=False,
        verify=True,
        headers=headers,
    ) as client:

        for scheme in ("https", "http"):
            url = f"{scheme}://{domain}/"

            for attempt in range(1, HTTP_MAX_ATTEMPTS + 1):
                try:
                    async with client.stream("GET", url) as response:
                        # Retry temporary server-side failures once.
                        if (
                            500 <= response.status_code < 600
                            and attempt < HTTP_MAX_ATTEMPTS
                        ):
                            logger.warning(
                                "Received HTTP %s from %s "
                                "(attempt %s/%s). Retrying.",
                                response.status_code,
                                url,
                                attempt,
                                HTTP_MAX_ATTEMPTS,
                            )
                            await asyncio.sleep(
                                HTTP_RETRY_DELAY_SECONDS
                            )
                            continue

                        title = await _read_limited_html(
                            response,
                            max_response_bytes,
                        )

                        elapsed = (
                            time.perf_counter() - started
                        ) * 1000

                        return {
                            "http_status": response.status_code,
                            "page_title": title,
                            "response_time_ms": round(elapsed, 2),
                        }

                except ResponseTooLargeError:
                    # A larger response is not a transient network failure.
                    # Do not retry it or fall back to another scheme.
                    raise

                except httpx.TransportError as exc:
                    last_error = exc

                    if attempt < HTTP_MAX_ATTEMPTS:
                        logger.warning(
                            "Transient HTTP network error for %s "
                            "(attempt %s/%s): %s. Retrying.",
                            url,
                            attempt,
                            HTTP_MAX_ATTEMPTS,
                            exc,
                        )
                        await asyncio.sleep(
                            HTTP_RETRY_DELAY_SECONDS
                        )
                        continue

                    # After HTTPS retries are exhausted, try HTTP.
                    break

    raise RuntimeError(
        "HTTP/HTTPS request failed: "
        f"{type(last_error).__name__}: {last_error}"
    )
