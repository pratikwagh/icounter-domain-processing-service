"""Behavioral tests for api scenarios functionality."""

from unittest.mock import patch
from typing import Any

import pytest


def test_root_endpoint(client) -> None:
    """
        Given: root endpoint.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.get("/")

    assert response.status_code == 200


def test_health_endpoint(client) -> None:
    """
        Given: health endpoint.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.get("/health")

    assert response.status_code == 200


def test_domains_field_missing(client) -> None:
    """
        Given: domains field missing.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post("/jobs", json={})

    assert response.status_code == 422


def test_domains_field_null(client) -> None:
    """
        Given: domains field null.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post("/jobs", json={"domains": None})

    assert response.status_code == 422


def test_empty_domain_list(client) -> None:
    """
        Given: empty domain list.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post("/jobs", json={"domains": []})

    assert response.status_code == 422


def test_empty_domain_name(client) -> None:
    """
        Given: empty domain name.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post(
        "/jobs",
        json={"domains": ["example.com", ""]},
    )

    assert response.status_code == 422


def test_whitespace_domain_name(client) -> None:
    """
        Given: whitespace domain name.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post(
        "/jobs",
        json={"domains": ["example.com", "   "]},
    )

    assert response.status_code == 422


def test_more_than_100_domains(client) -> None:
    """
        Given: more than 100 domains.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    domains = [f"example{i}.com" for i in range(101)]

    response = client.post(
        "/jobs",
        json={"domains": domains},
    )

    assert response.status_code == 422


def test_single_domain(client) -> None:
    """
        Given: single domain.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post(
        "/jobs",
        json={"domains": ["example.com"]},
    )

    assert response.status_code == 202

    body = response.json()

    assert body["job_id"]
    assert body["status"] in {"queued", "pending"}
    assert body["total_domains"] == 1


def test_multiple_domains(client) -> None:
    """
        Given: multiple domains.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    domains = [
        "example.com",
        "python.org",
        "openai.com",
    ]

    response = client.post(
        "/jobs",
        json={"domains": domains},
    )

    assert response.status_code == 202

    body = response.json()

    assert body["job_id"]
    assert body["total_domains"] == 3


def test_duplicate_domains_are_deduplicated(client) -> None:
    """
        Given: duplicate domains are deduplicated.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post(
        "/jobs",
        json={
            "domains": [
                "example.com",
                "EXAMPLE.COM",
                "example.com.",
            ]
        },
    )

    assert response.status_code == 202

    body = response.json()

    assert body["total_domains"] == 1


def test_invalid_domain_syntax_is_accepted_for_processing(client) -> None:
    """
        Given: invalid domain syntax is accepted for processing.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post(
        "/jobs",
        json={"domains": ["not a valid domain"]},
    )

    # Syntax validation happens in the worker so that the job
    # can return an individual failure result for this domain.
    assert response.status_code == 202


def test_get_unknown_job_returns_404(client) -> None:
    """
        Given: get unknown job returns 404.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.get("/jobs/unknown-job-id")

    assert response.status_code == 404


def test_get_existing_job(client) -> None:
    """
        Given: get existing job.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    create_response = client.post(
        "/jobs",
        json={"domains": ["example.com"]},
    )

    assert create_response.status_code == 202

    job_id = create_response.json()["job_id"]

    response = client.get(f"/jobs/{job_id}")

    assert response.status_code == 200

    body = response.json()

    assert body["job"]["job_id"] == job_id
    assert body["job"]["total_domains"] == 1


def test_job_creation_with_multiple_unique_domains(client) -> None:
    """
        Given: job creation with multiple unique domains.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    response = client.post(
        "/jobs",
        json={
            "domains": [
                "example.com",
                "python.org",
                "openai.com",
                "github.com",
            ]
        },
    )

    assert response.status_code == 202
    assert response.json()["total_domains"] == 4


def test_concurrent_job_submissions_receive_unique_ids(client) -> None:
    """
        Given: concurrent job submissions receive unique ids.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    from concurrent.futures import ThreadPoolExecutor

    request_count = 12

    def submit(index) -> None:
        """Given: submit.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return client.post(
            "/jobs",
            json={"domains": [f"example{index}.com"]},
        )

    with ThreadPoolExecutor(max_workers=request_count) as executor:
        responses = list(
            executor.map(submit, range(request_count))
        )

    assert all(response.status_code == 202 for response in responses)

    job_ids = [
        response.json()["job_id"]
        for response in responses
    ]

    assert len(job_ids) == request_count
    assert len(set(job_ids)) == request_count


def test_queue_full_returns_503_without_creating_job(
    tmp_path,
    monkeypatch,
) -> None:
    """
        Given: queue full returns 503 without creating job.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    import sqlite3
    from concurrent.futures import ThreadPoolExecutor

    from fastapi.testclient import TestClient

    import app.main as main_module

    class IdleWorker:
        """Worker stub that deliberately leaves queued jobs untouched."""

        def __init__(self, queue, database) -> None:
            """Given:   init  .
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
            self.queue = queue
            self.database = database

        async def run_forever(self) -> None:
            """Given: run forever.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
            import asyncio
            await asyncio.Event().wait()

    monkeypatch.setattr(
        main_module,
        "JobWorker",
        IdleWorker,
    )

    database_path = tmp_path / "queue_full.db"

    application = main_module.create_app(
        db_path=str(database_path),
        start_workers=True,
        retention_days=None,
        queue_maxsize=1,
    )

    with TestClient(application) as test_client:
        def submit(domain) -> None:
            """Given: submit.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
            return test_client.post(
                "/jobs",
                json={"domains": [domain]},
            )

        # Both requests race for the only available queue slot.
        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(
                executor.map(
                    submit,
                    ["example.com", "python.org"],
                )
            )

        assert sorted(
            response.status_code for response in responses
        ) == [202, 503]

        assert application.state.job_queue.qsize() == 1

    # Only the accepted request should have a persisted job.
    with sqlite3.connect(database_path) as connection:
        job_count = connection.execute(
            "SELECT COUNT(*) FROM jobs"
        ).fetchone()[0]

    assert job_count == 1
