"""Behavioral tests for database functionality."""

import aiosqlite
from typing import Any
import pytest

from app.database import Database


@pytest.mark.asyncio
async def test_retention_uses_updated_at_and_cascades_domain_results(tmp_path) -> None:
    """
        Given: retention uses updated at and cascades domain results.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    database = Database(str(tmp_path / "retention.db"))
    await database.initialize()

    await database.create_job(
        "recently-updated",
        ["example.com"],
    )
    await database.create_job(
        "inactive-job",
        ["python.org"],
    )

    async with database.connection() as connection:
        # Both jobs were created more than 10 days ago.
        # Only the second job has also been inactive for more than 10 days.
        await connection.execute(
            """
            UPDATE jobs
            SET status = 'completed',
                created_at = datetime('now', '-20 days'),
                updated_at = datetime('now', '-2 days')
            WHERE job_id = 'recently-updated'
            """
        )
        await connection.execute(
            """
            UPDATE jobs
            SET status = 'completed',
                created_at = datetime('now', '-20 days'),
                updated_at = datetime('now', '-11 days')
            WHERE job_id = 'inactive-job'
            """
        )
        await connection.commit()

    deleted = await database.cleanup_expired_jobs(
        retention_days=10
    )

    assert deleted == 1
    assert await database.get_job("recently-updated") is not None
    assert await database.get_job("inactive-job") is None

    async with database.connection() as connection:
        cursor = await connection.execute(
            """
            SELECT COUNT(*)
            FROM domain_results
            WHERE job_id = 'inactive-job'
            """
        )
        remaining_domain_rows = (await cursor.fetchone())[0]

    assert remaining_domain_rows == 0
