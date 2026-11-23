"""Behavioral tests for worker functionality."""


import asyncio
from typing import Any

import pytest

import app.worker as worker_module
from app.worker import (
    JobWorker,
    inspect_domain,
    is_valid_domain,
    normalize_domain,
)


def test_normalize_domain() -> None:
    """
        Given: normalize domain.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    assert normalize_domain(" Google.COM. ") == "google.com"
    assert normalize_domain("PYTHON.ORG") == "python.org"


def test_normalize_unicode_domain() -> None:
    """
        Given: normalize unicode domain.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    normalized = normalize_domain("münich.com")

    assert normalized == "xn--mnich-kva.com"


def test_valid_domain() -> None:
    """
        Given: valid domain.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    assert is_valid_domain("example.com")
    assert is_valid_domain("google.com")
    assert is_valid_domain("sub.example.co.in")


def test_invalid_domain() -> None:
    """
        Given: invalid domain.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    assert not is_valid_domain("not a domain")
    assert not is_valid_domain("localhost")
    assert not is_valid_domain("-example.com")
    assert not is_valid_domain("example..com")
    assert not is_valid_domain("https://example.com")


@pytest.mark.asyncio
async def test_invalid_domain_returns_failed_result() -> None:
    """
        Given: invalid domain returns failed result.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    result = await inspect_domain("not a domain")

    assert result["status"] == "failed"
    assert result["error"] == "Invalid domain format"


@pytest.mark.asyncio
async def test_successful_domain_inspection(monkeypatch) -> None:
    """
        Given: successful domain inspection.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    async def mock_dns(domain) -> None:
        """Given: mock dns.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return (
            ["93.184.216.34"],
            {
                "A": ["93.184.216.34"],
                "TXT": [],
                "CNAME": [],
                "MX": [],
                "NS": [],
            },
        )

    async def mock_http(domain) -> None:
        """Given: mock http.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return {
            "http_status": 200,
            "page_title": "Example Domain",
            "response_time_ms": 120,
        }

    monkeypatch.setattr(
        worker_module,
        "lookup_dns",
        mock_dns,
    )

    monkeypatch.setattr(
        worker_module,
        "fetch_page",
        mock_http,
    )

    result = await inspect_domain("example.com")

    assert result["status"] == "completed"
    assert result["ip_addresses"] == ["93.184.216.34"]
    assert result["http_status"] == 200
    assert result["page_title"] == "Example Domain"
    assert result["error"] is None


@pytest.mark.asyncio
async def test_dns_failure_but_http_success(monkeypatch) -> None:
    """
        Given: dns failure but http success.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    async def mock_dns(domain) -> None:
        """Given: mock dns.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        raise TimeoutError("DNS server did not respond")

    async def mock_http(domain) -> None:
        """Given: mock http.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return {
            "http_status": 200,
            "page_title": "Example Domain",
            "response_time_ms": 100,
        }

    monkeypatch.setattr(
        worker_module,
        "lookup_dns",
        mock_dns,
    )

    monkeypatch.setattr(
        worker_module,
        "fetch_page",
        mock_http,
    )

    result = await inspect_domain("example.com")

    # HTTP inspection succeeded, so the domain inspection
    # is completed, but DNS failure is recorded as a warning.
    assert result["status"] == "completed"
    assert result["http_status"] == 200
    assert "DNS lookup failed" in result["error"]


@pytest.mark.asyncio
async def test_http_failure(monkeypatch) -> None:
    """
        Given: http failure.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    async def mock_dns(domain) -> None:
        """Given: mock dns.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return (
            ["93.184.216.34"],
            {"A": ["93.184.216.34"]},
        )

    async def mock_http(domain) -> None:
        """Given: mock http.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        raise ConnectionError("Website is unreachable")

    monkeypatch.setattr(
        worker_module,
        "lookup_dns",
        mock_dns,
    )

    monkeypatch.setattr(
        worker_module,
        "fetch_page",
        mock_http,
    )

    result = await inspect_domain("example.com")

    assert result["status"] == "failed"
    assert result["ip_addresses"] == ["93.184.216.34"]
    assert "Website is unreachable" in result["error"]


class FakeDatabase:
    def __init__(self, domains) -> None:
        """Given:   init  .
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        self.domains = domains
        self.saved_results = {}
        self.processing_ids = []
        self.job_status = None
        self.refreshed = False
        self.batch_returned = False

    async def update_job_status(self, job_id, status) -> None:
        """Given: update job status.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        self.job_status = status

    async def get_pending_domains(self, job_id, batch_size) -> None:
        """Given: get pending domains.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        if self.batch_returned:
            return []

        self.batch_returned = True
        return self.domains[:batch_size]

    async def mark_processing(self, domain_id) -> None:
        """Given: mark processing.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        self.processing_ids.append(domain_id)

    async def save_domain_result(self, domain_id, result) -> None:
        """Given: save domain result.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        self.saved_results[domain_id] = result

    async def refresh_job_counts(self, job_id) -> None:
        """Given: refresh job counts.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        self.refreshed = True


@pytest.mark.asyncio
async def test_worker_constructor_order() -> None:
    """
        Given: worker constructor order.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    queue = asyncio.Queue()
    database = FakeDatabase([])

    worker = JobWorker(queue, database)

    assert worker.queue is queue
    assert worker.database is database


@pytest.mark.asyncio
async def test_worker_saves_timeout_result(monkeypatch) -> None:
    """
        Given: worker saves timeout result.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    monkeypatch.setattr(
        worker_module,
        "DOMAIN_TIMEOUT_SECONDS",
        0.01,
    )

    async def mock_inspect_domain(domain) -> None:
        """Given: mock inspect domain.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        await asyncio.sleep(1)

    monkeypatch.setattr(
        worker_module,
        "inspect_domain",
        mock_inspect_domain,
    )

    database = FakeDatabase(
        [{"id": 1, "domain": "slow.example.com"}]
    )

    worker = JobWorker(
        asyncio.Queue(),
        database,
        concurrency=2,
    )

    await worker.process_job("job-timeout")

    result = database.saved_results[1]

    assert result["status"] == "failed"
    assert result["error"] == "Processing timed out"
    assert database.refreshed is True


@pytest.mark.asyncio
async def test_one_domain_timeout_does_not_stop_other_domains(
    monkeypatch,
) -> None:
    """
        Given: one domain timeout does not stop other domains.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    monkeypatch.setattr(
        worker_module,
        "DOMAIN_TIMEOUT_SECONDS",
        0.05,
    )

    async def mock_inspect_domain(domain) -> None:
        """Given: mock inspect domain.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        if domain == "slow.example.com":
            await asyncio.sleep(1)

        return {
            "status": "completed",
            "page_title": "Example",
        }

    monkeypatch.setattr(
        worker_module,
        "inspect_domain",
        mock_inspect_domain,
    )

    database = FakeDatabase(
        [
            {"id": 1, "domain": "slow.example.com"},
            {"id": 2, "domain": "fast.example.com"},
        ]
    )

    worker = JobWorker(
        asyncio.Queue(),
        database,
        concurrency=2,
    )

    await worker.process_job("job-partial")

    slow_result = database.saved_results[1]
    fast_result = database.saved_results[2]

    assert slow_result["status"] == "failed"
    assert slow_result["error"] == "Processing timed out"

    assert fast_result["status"] == "completed"

    assert database.refreshed is True


@pytest.mark.asyncio
async def test_unexpected_worker_exception_is_saved(monkeypatch) -> None:
    """
        Given: unexpected worker exception is saved.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    async def mock_inspect_domain(domain) -> None:
        """Given: mock inspect domain.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        raise RuntimeError("Unexpected processing error")

    monkeypatch.setattr(
        worker_module,
        "inspect_domain",
        mock_inspect_domain,
    )

    database = FakeDatabase(
        [{"id": 1, "domain": "example.com"}]
    )

    worker = JobWorker(
        asyncio.Queue(),
        database,
    )

    await worker.process_job("job-error")

    result = database.saved_results[1]

    assert result["status"] == "failed"
    assert "Unexpected processing error" in result["error"]
