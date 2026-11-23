"""Shared pytest fixtures for the service test suite."""

import pytest
from typing import Any
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def app(tmp_path) -> None:
    """
        Given: app.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    database_path = tmp_path / "test_jobs.db"

    application = create_app(
        db_path=str(database_path),
        start_workers=False,
        retention_days=None,
    )

    return application


@pytest.fixture
def client(app) -> None:
    """
        Given: client.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    with TestClient(app) as test_client:
        yield test_client