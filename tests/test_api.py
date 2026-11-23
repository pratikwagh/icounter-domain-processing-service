"""Behavioral tests for api functionality."""

from fastapi.testclient import TestClient
from typing import Any

from app.main import create_app


def test_health_and_job_creation(tmp_path) -> None:
    """
        Given: health and job creation.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    app = create_app(str(tmp_path / "test.db"), start_workers=False)
    with TestClient(app) as client:
        health = client.get("/health").json()
        assert health["status"] == "ok"
        assert health["workers_enabled"] is False
        assert health["queue_size"] == 0

        response = client.post("/jobs", json={"domains": ["Example.com", "example.com.", "bad input"]})
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "pending"
        assert body["total_domains"] == 2

        result = client.get(f"/jobs/{body['job_id']}")
        assert result.status_code == 200
        assert len(result.json()["results"]) == 2


def test_empty_domains_rejected(tmp_path) -> None:
    """
        Given: empty domains rejected.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    app = create_app(str(tmp_path / "test.db"), start_workers=False)
    with TestClient(app) as client:
        response = client.post("/jobs", json={"domains": []})
        assert response.status_code == 422
