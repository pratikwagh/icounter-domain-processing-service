# Domain Processing Service

An asynchronous Python API that accepts domain-inspection jobs, processes domains in the background, and persists results.

## Features

- `POST /jobs`: submit a list of domains and receive a job ID immediately (`202 Accepted`).
- `GET /jobs/{job_id}`: retrieve job status, counts, and paginated domain results.
- DNS lookups: A, TXT, CNAME, MX and NS.
- HTTP/HTTPS inspection: status code, response time, and HTML page title.
- Per-domain failures do not stop the rest of the job.
- Domain normalization and duplicate removal within a job.
- SQLite persistence and bounded background processing.\n- One retry for transient DNS/network failures and HTTP 5xx responses.\n- Streaming HTTP response-size limit (2 MB by default).\n- Automatic cleanup of terminal jobs after 10 days without an update.
- Basic SSRF protection: reject hostnames resolving to non-public IP addresses; redirects are not followed.

## Requirements

Python 3.10+ (tested design target: Python 3.13).

## Setup

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

### macOS/Linux

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m uvicorn app.main:app --reload
```

The API is available at `http://127.0.0.1:8000`. Interactive documentation: `http://127.0.0.1:8000/docs`.

## Example

Submit:

```bash
curl -X POST http://127.0.0.1:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"domains":["example.com","python.org","example.com"]}'
```

Example response:

```json
{
  "job_id": "generated-uuid",
  "status": "pending",
  "total_domains": 2
}
```

Retrieve results:

```bash
curl "http://127.0.0.1:8000/jobs/generated-uuid?limit=100&offset=0"
```

## Architecture

- **FastAPI** validates requests and exposes the REST API.
- **Uvicorn** is the ASGI server that runs the application.
- **asyncio.Queue** holds job IDs; two background worker coroutines consume jobs.
- Each worker reads domains from SQLite in batches of 50 and creates at most 50 tasks per batch, with a semaphore limiting active inspections to 20.\n- Job admission is protected by an asynchronous lock so concurrent requests cannot overbook the bounded queue.
- **dnspython** performs asynchronous DNS queries.
- **httpx** performs asynchronous HTTP requests.
- **BeautifulSoup** extracts the HTML title.
- **aiosqlite** persists jobs and results.

The database is the source of truth for job/result state. The in-memory queue is only a local scheduling mechanism.

## Status semantics

Job statuses: `pending`, `in_progress`, `completed`, `partially_completed`, `failed` (and `completed_with_errors` for compatibility with older records).

Domain statuses: `pending`, `processing`, `completed`, `failed`.

A reachable website returning HTTP 404 or 500 is still a successfully inspected domain: the HTTP status is recorded and the domain result is `completed`. DNS record types that do not exist are represented as empty arrays. A website inspection failure is recorded on that domain and does not stop other domains.

## Tests

```bash
python -m pytest -q
```

## Assumptions and limitations

- Input values are hostnames, not full URLs.
- Duplicate domains are normalized and removed within a job.
- The request accepts at most 100 domain strings. Results are returned using pagination (`limit` and `offset`).
- The local queue is in memory. On process startup, unfinished records are reset and pending jobs are re-enqueued. A sudden process crash can delay work until restart; this is not equivalent to a durable broker.
- SQLite is appropriate for a small, single-instance demonstration. It serializes writes and is not intended as the production database for high write throughput or multiple application instances.
- The HTTP client does not follow redirects. This avoids redirect-based SSRF paths in this simple implementation. A production implementation should also pin/validate the destination IP at connection time to defend against DNS rebinding.
- DNS results are gathered independently. Some record types may legitimately be absent.
- External DNS/HTTP operations depend on network connectivity and remote server behavior.\n- Transient DNS errors, HTTP transport errors, and HTTP 5xx responses are retried once after a short delay. Invalid domains and non-transient HTTP statuses are not retried.\n- HTTP response bodies are streamed with a 2 MB default maximum (`HTTP_MAX_RESPONSE_BYTES` can override it). The limit is enforced even when `Content-Length` is absent or inaccurate.\n- Terminal jobs are eligible for deletion when their `updated_at` timestamp is older than the 10-day retention period (`JOB_RETENTION_DAYS` can override it). Pending and in-progress jobs are not deleted.

## Production improvements

For large workloads (e.g. hundreds of thousands or millions of domains), use PostgreSQL, a durable queue such as RabbitMQ/Redis-backed task processing, independently scalable worker processes, request-size limits, rate limiting, metrics/tracing, and bulk database writes. The current retry policy is intentionally limited to one retry. Use a streaming/batched ingestion API rather than one enormous JSON request. Scale API instances behind a load balancer. The current implementation demonstrates bounded concurrency, not guaranteed million-domain throughput.
