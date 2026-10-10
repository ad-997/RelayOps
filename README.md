# RelayOps — durable webhook delivery

A working multi-service engineering project: **React → Java Spring Boot → Python FastAPI → transactional SQL queue → HMAC-signed HTTP delivery**.

It solves the delivery side of a common integration failure: accepting an event does not mean the downstream receiver is available. Events persist independently of delivery, producers can safely retry their submission, transient failures recover automatically, and persistent failures remain inspectable and replayable.

Supports operator-configured real destinations and a separate fulfillment receiver example. Local integration checks pass; public deployment is pending hosting access. Customer usage and production capacity are not claimed.

## Implemented

- React operations dashboard with state filters, attempt history, test dispatch, and manual dead-letter replay.
- Java validation boundary and downstream HTTP error propagation.
- FastAPI service with OpenAPI docs; API key enforced at both the public Java gateway and worker when configured. Production startup requires durable database configuration and secrets.
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

Open **http://127.0.0.1:8083** for the React dashboard and **http://127.0.0.1:8082/docs** for worker API docs. SQLite defaults to `relay.db` in the project directory. Set RELAY_API_KEY before exposing the gateway. Production mode refuses missing key, signing secret or database configuration.

Choose a test destination and dispatch an event. Healthy accepts immediately. Flaky returns 503 twice before accepting. Offline returns 503 on every attempt and enters the failed queue. Inspect to see the attempt history; select a dead event to replay it.

For a persistent signing key or service-to-service authentication, set the same `RELAY_SECRET` and `RELAY_API_KEY` when starting services. The Java gateway reads `RELAY_API_KEY`; the browser must supply it for protected requests. The dashboard keeps it in memory only.

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

API callers choose a destination name, never an arbitrary URL. Operators register destinations through RELAY_DESTINATIONS.

## Delivery guarantees and tradeoffs

This is **at-least-once** delivery, not exactly-once delivery. If a worker crashes after the receiver accepts a request but before the SQL acknowledgment, lease recovery can send it again. Consumers must deduplicate `X-Relay-Event`. Stale leases are fenced at acknowledgment, not at the remote receiver. Attempt duration measures outbound HTTP only; enqueue p95 measures Java gateway through SQL acceptance.

Replay resets the attempt count for a new retry cycle while preserving old attempt records. Success and failure counters count current event states; attempt counters count all recorded attempts. SQLite write serialization makes it convenient for local learning, not an unbounded scale claim. The worker is integrated into the Python service lifespan for an easy local run.

## Engineering extensions worth owning

Further hardening for a multi-customer service: per-tenant authorization, per-destination secret rotation, request-time DNS pinning, graceful shutdown draining, schema migrations, retention policies, tenant isolation, quotas, metrics export, and failure-injection tests against MySQL. Operator-configured destinations are trusted configuration; only administrators should change them. DNS is checked at startup, not pinned per delivery. Separate worker deployments and load testing are further work; no broker, Redis, Kubernetes, or production scalability claims are made here.

## Interview discussion prompts

Why is an idempotency key tied to both destination and payload? What happens if the process dies after HTTP 200? Why do leases need fencing tokens? Which errors should retry? Why preserve a dead-letter record instead of deleting it? What changes when moving from SQLite to MySQL? Which guarantees require cooperation from the consumer?


## Integration

Use case: an order service has accepted payment, but fulfillment is temporarily unavailable. RelayOps persists the paid-order event, retries delivery, and retains failures for replay after fulfillment recovers. It handles delivery; receivers still own their business logic.

Configure the gateway and worker with the same RELAY_API_KEY. Configure the worker with RELAY_SECRET and an operator-owned destination map:

```sh
export RELAY_DESTINATIONS='{"fulfillment":"https://your-fulfillment-host.example/orders"}'
```

The URL above is a placeholder to replace with your receiver, not a deployed service. Production destinations require HTTPS and publicly resolving addresses. Redirects are not followed. The receiver verifies the HMAC signature and deduplicates X-Relay-Event; see examples/order_receiver.py.

Submit through the public Java gateway:

```sh
curl "$RELAY_URL/api/events" \
  -H "X-API-Key: $RELAY_API_KEY" -H 'Content-Type: application/json' \
  -d '{"key":"paid-order-42","endpoint":"fulfillment","payload":{"orderId":42,"status":"paid"}}'
```

A successful enqueue response means the event is stored, not yet delivered. Poll the event ledger or inspect attempt history. Reusing the same key and data returns the existing event; changing data for that key returns 409. Delivery is at least once, so consumer deduplication remains necessary.

## Separate receiver acceptance check

After building Java and installing Python requirements, run:

```sh
.venv/bin/python verify_integration.py
```

This starts a separate fulfillment application and the Java/Python services on localhost ports 18083/18082/18084, with temporary databases and randomly generated keys. It checks unauthorized rejection, duplicate suppression, four failed delivery attempts, history persistence across a worker restart, and successful manual replay after recovery. The receiver stores exactly one order. See integration-results.json for the observed result; it is local evidence, not hosted evidence.

## Hosting

The root Dockerfile builds the React UI and Java gateway and runs the Python worker privately in the same container. start_services.py terminates the container if either process exits. render.yaml defines a free Render service with an external Neon Free Postgres database, generated secrets, and /api/healthz health checks.

Only the Java port is public; the Python worker binds to localhost. The hosted queue lives in Neon Postgres, outside the temporary container filesystem. Set RELAY_DB to the Neon connection string with SSL enabled. All /api routes except the basic health check require the API key. In Render, enter RELAY_DESTINATIONS as {} to run only clearly labeled simulated receivers, or supply your actual HTTPS integration map. Set RELAY_DEMO_RECEIVERS=false to remove fixtures.

This single-instance edition has downtime during restart/deploy and a single API key for trusted operators. It is suitable for a small controlled integration, not an open multi-tenant SaaS. Monitor database usage and take backups; retention and automatic archival are not implemented. Never send actual payment credentials or sensitive customer data into portfolio demos.


### Free-hosting behavior

Render Free sleeps after 15 minutes without incoming traffic and may take roughly a minute to wake. Retry processing pauses while asleep and resumes when the service wakes. Events remain in Neon Postgres; the queue’s lease recovery restores interrupted deliveries. This is a working hobby deployment with delayed recovery during sleep, not a continuously available production service. There is no keep-awake workaround. Both accounts stay on their free plans; usage limits can suspend access rather than provide unlimited capacity.

The public dashboard requires the service API key from Render’s Environment page. Do not publish that key or include it in the portfolio. An authenticated operator can use the API from another application; destination URLs are registered by the operator in Render’s RELAY_DESTINATIONS setting.
