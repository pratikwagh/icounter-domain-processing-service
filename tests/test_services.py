"""Behavioral tests for services functionality."""

import asyncio
from typing import Any
from collections import defaultdict

import dns.exception
import dns.resolver
import httpx
import pytest

from app.services import dns_service, http_service


class FakeDNSRecord:
    def to_text(self) -> None:
        """Given: to text.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return "93.184.216.34"


@pytest.mark.asyncio
async def test_dns_transient_timeout_is_retried(monkeypatch) -> None:
    """
        Given: dns transient timeout is retried.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    calls = defaultdict(int)

    class FakeResolver:
        lifetime = None

        async def resolve(self, domain, record_type, lifetime) -> None:
            """
                Given: resolve.
                Then: The expected behavior is verified.
                Response: Assertions confirm the expected result.
            """
            calls[record_type] += 1

            if record_type == "A":
                if calls[record_type] == 1:
                    raise dns.exception.Timeout()
                return [FakeDNSRecord()]

            raise dns.resolver.NoAnswer()

    monkeypatch.setattr(
        dns_service.dns.asyncresolver,
        "Resolver",
        FakeResolver,
    )
    monkeypatch.setattr(
        dns_service,
        "DNS_RETRY_DELAY_SECONDS",
        0,
    )

    ips, records = await dns_service.lookup_dns("example.com")

    assert calls["A"] == 2
    assert ips == ["93.184.216.34"]
    assert records["A"] == ["93.184.216.34"]


@pytest.mark.asyncio
async def test_http_5xx_response_is_retried(monkeypatch) -> None:
    """
        Given: http 5xx response is retried.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    calls = 0

    async def no_op_validation(domain) -> None:
        """Given: no op validation.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return None

    def handler(request) -> None:
        """Given: handler.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        nonlocal calls
        calls += 1

        if calls == 1:
            return httpx.Response(503, request=request)

        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"<html><title>Example</title></html>",
            request=request,
        )

    original_async_client = httpx.AsyncClient

    def client_factory(**kwargs) -> None:
        """Given: client factory.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return original_async_client(
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    monkeypatch.setattr(
        http_service,
        "validate_public_destination",
        no_op_validation,
    )
    monkeypatch.setattr(
        http_service.httpx,
        "AsyncClient",
        client_factory,
    )
    monkeypatch.setattr(
        http_service,
        "HTTP_RETRY_DELAY_SECONDS",
        0,
    )

    result = await http_service.fetch_page(
        "example.com",
        max_response_bytes=1024,
    )

    assert calls == 2
    assert result["http_status"] == 200
    assert result["page_title"] == "Example"


@pytest.mark.asyncio
async def test_http_response_content_length_limit(monkeypatch) -> None:
    """
        Given: http response content length limit.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    async def no_op_validation(domain) -> None:
        """Given: no op validation.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return None

    def handler(request) -> None:
        """Given: handler.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return httpx.Response(
            200,
            headers={
                "content-type": "text/html",
                "content-length": "100",
            },
            content=b"x" * 100,
            request=request,
        )

    original_async_client = httpx.AsyncClient

    def client_factory(**kwargs) -> None:
        """Given: client factory.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return original_async_client(
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    monkeypatch.setattr(
        http_service,
        "validate_public_destination",
        no_op_validation,
    )
    monkeypatch.setattr(
        http_service.httpx,
        "AsyncClient",
        client_factory,
    )

    with pytest.raises(http_service.ResponseTooLargeError):
        await http_service.fetch_page(
            "example.com",
            max_response_bytes=10,
        )


@pytest.mark.asyncio
async def test_http_streamed_body_limit_without_content_length(monkeypatch) -> None:
    """
        Given: http streamed body limit without content length.
        Then: The expected behavior is verified.
        Response: Assertions confirm the expected result.
    """
    async def no_op_validation(domain) -> None:
        """Given: no op validation.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return None

    def handler(request) -> None:
        """Given: handler.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"x" * 100,
            request=request,
        )

    original_async_client = httpx.AsyncClient

    def client_factory(**kwargs) -> None:
        """Given: client factory.
    Then: The expected behavior is verified.
    Response: Assertions confirm the expected result."""
        return original_async_client(
            transport=httpx.MockTransport(handler),
            **kwargs,
        )

    monkeypatch.setattr(
        http_service,
        "validate_public_destination",
        no_op_validation,
    )
    monkeypatch.setattr(
        http_service.httpx,
        "AsyncClient",
        client_factory,
    )

    with pytest.raises(http_service.ResponseTooLargeError):
        await http_service.fetch_page(
            "example.com",
            max_response_bytes=10,
        )
