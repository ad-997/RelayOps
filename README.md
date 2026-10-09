# RelayOps — webhook reliability lab

A working multi-service engineering project: **React → Java Spring Boot → Python FastAPI → transactional SQL queue → HMAC-signed HTTP delivery**.

It solves the delivery side of a common integration failure: accepting an event does not mean the downstream receiver is available. Events persist independently of delivery, producers can safely retry their submission, transient failures recover automatically, and persistent failures remain inspectable and replayable.

This is a local engineering lab with controlled failure scenarios and reproducible measurements. Production deployment, customer usage, and MySQL performance are not claimed.

## Implemented

- React operations dashboard with state filters, attempt history, test dispatch, and manual dead-letter replay.
- Java validation boundary and downstream HTTP error propagation.
- FastAPI service with OpenAPI docs and optional service API key.
- SQLAlchemy queue supporting SQLite and a MySQL connection URL.
- Unique idempotency keys with payload + destination fingerprints and conflict detection.
- Transactional claims, worker lease expiry, and stale-worker acknowledgment fencing.
- Eight concurrent deliveries; exponential backoff with jitter, four attempts per retry cycle.
- Retry classification for transport errors, 408, 425, 429, and 5xx. Other 4xx enter dead letters immediately.
- HMAC-SHA256 payload signatures and five-minute receiver timestamp validation.
- Attempt history with HTTP status, duration, and outcome.
- Healthy, twice-failing, and persistently failing local receiver fixtures.
- Unit/concurrency/API tests and an end-to-end HTTP benchmark.
- Container recipes for React/Spring Boot, FastAPI, and MySQL.

## Measured outcomes

See `benchmark-results.json` and `benchmark.py`. One actual localhost run through the Java/Python services tested **200 synthetic unique events** with 8 concurrent producers:

| Observation | Result |
|---|---:|
| Healthy events delivered | 80 / 80 |
| Events failing twice, then recovered | 100 / 100 |
| Persistent failures retained after four attempts | 20 / 20 |
| Repeated producer submissions suppressed | 200 / 200 |
| Delivery attempts recorded | 460 |
| Changed payload using an existing key | HTTP 409 |
| p95 enqueue latency | 267.93 ms |
| Ingestion time for 200 requests | 2.575 s |

These are synthetic local results on macOS/ARM, Python 3.14.6, Java 25, SQLite WAL. They are not customer outcomes or production load capacity. The benchmark includes real HTTP, real SQL commits, and real retry delays. A React-style request contract was used by the benchmark script; the browser UI was verified separately. MySQL/containers are supplied as a deployment recipe and have not been executed in this environment.

## Run locally

Prerequisites: Python 3.12+, Node 22+, Java 17+, Maven.

From this directory:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock.txt
cd dashboard
npm ci
npm run build
cd ../control-plane
mvn package
cd ..
```

Terminal 1, from the project root:

```sh
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8082
```

Terminal 2:

```sh
java -jar control-plane/target/control-plane-1.0.0.jar
```

Open **http://127.0.0.1:8083** for the React dashboard and **http://127.0.0.1:8082/docs** for worker API docs. SQLite defaults to `relay.db` in the project directory. Do not expose the unauthenticated local gateway to the internet.

Choose a test destination and dispatch an event. Healthy accepts immediately. Flaky returns 503 twice before accepting. Offline returns 503 on every attempt and enters the failed queue. Inspect to see the attempt history; select a dead event to replay it.

For a persistent signing key or service-to-service authentication, set the same `RELAY_SECRET` and `RELAY_API_KEY` when starting services. The Java gateway reads `RELAY_API_KEY`; it does not expose it to the browser.

## Test and reproduce the benchmark

```sh
.venv/bin/pytest tests -q
cd control-plane
mvn test
```

Use a fresh database for the benchmark: stop the worker, then start it with a new path rather than deleting data:

```sh
RELAY_DB=benchmark-new.db .venv/bin/uvicorn app:app --port 8082
```

Keep the Java service running and execute:

```sh
.venv/bin/python benchmark.py --out my-results.json
```

The script rejects a nonempty database, submits a fixed receiver mix, resubmits all keys, waits for terminal outcomes, checks the counts, and records observed timings. Results vary with machine load and runtime warm-up.

## MySQL container recipe

Copy `.env.example` to `.env`, replace all values with randomly generated URL-safe values, then run:

```sh
docker compose up --build
```

The gateway binds to localhost port 8083. Worker and database remain internal to the compose network. MySQL data uses a persistent volume. These recipes are unverified here; validate them before adding MySQL performance or deployment claims to a resume.

## API surface

- `POST /api/events`: `{ "key": "order-42", "endpoint": "flaky", "payload": {"order":42} }`
- `GET /api/events?status=dead&limit=100`
- `GET /api/metrics`
- `GET /api/events/{uuid}/attempts`
- `POST /api/events/{uuid}/replay`

The local edition deliberately uses registered receiver fixtures. It does not accept arbitrary callback URLs, avoiding an unrestricted server-side URL fetch feature.

## Delivery guarantees and tradeoffs

This is **at-least-once** delivery, not exactly-once delivery. If a worker crashes after the receiver accepts a request but before the SQL acknowledgment, lease recovery can send it again. Consumers must deduplicate `X-Relay-Event`. Stale leases are fenced at acknowledgment, not at the remote receiver. Attempt duration measures outbound HTTP only; enqueue p95 measures Java gateway through SQL acceptance.

Replay resets the attempt count for a new retry cycle while preserving old attempt records. Success and failure counters count current event states; attempt counters count all recorded attempts. SQLite write serialization makes it convenient for local learning, not an unbounded scale claim. The worker is integrated into the Python service lifespan for an easy local run.

## Engineering extensions worth owning

Before public production use: operator authentication and authorization, configurable endpoint lifecycle with DNS-safe outbound policy, secret rotation, receiver idempotency records, graceful shutdown draining, schema migrations, retention policies, tenant isolation, quotas, metrics export, and failure-injection tests against MySQL. Separate worker deployments and load testing are further work; no broker, Redis, Kubernetes, or production scalability claims are made here.

## Interview discussion prompts

Why is an idempotency key tied to both destination and payload? What happens if the process dies after HTTP 200? Why do leases need fencing tokens? Which errors should retry? Why preserve a dead-letter record instead of deleting it? What changes when moving from SQLite to MySQL? Which guarantees require cooperation from the consumer?
