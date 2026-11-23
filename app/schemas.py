"""Application module for schemas functionality."""

from pydantic import BaseModel, Field, field_validator


class JobCreate(BaseModel):
    domains: list[str] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Between 1 and 100 domains are allowed per job.",
    )

    @field_validator("domains")
    @classmethod
    def validate_domains_not_empty(cls, domains: list[str]) -> list[str]:
        """Validate domains not empty."""
        if not domains:
            raise ValueError("At least one domain must be provided.")

        if any(not isinstance(domain, str) or not domain.strip()
               for domain in domains):
            raise ValueError("Domain names cannot be empty.")

        return domains


class JobCreated(BaseModel):
    job_id: str
    status: str
    total_domains: int


class DomainResultResponse(BaseModel):
    domain: str
    status: str
    result: dict | None = None
    error: str | None = None


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    total_domains: int
    completed_domains: int
    failed_domains: int
    results: list[DomainResultResponse]