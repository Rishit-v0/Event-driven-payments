# Event-Driven Order & Payment Processing System

A distributed order-processing backend built around an event-driven, microservice
architecture. Four independent services coordinate the lifecycle of an order
(creation, payment, inventory reservation, notification) by exchanging events over
Apache Kafka, never by calling each other directly.

The project exists to demonstrate the hard parts of distributed systems correctly:
the transactional outbox pattern, a choreography-based saga with compensating
transactions, idempotent consumers under at-least-once delivery, row-level locking
for concurrency safety, and dead-letter-queue isolation for poison messages.

## Architecture

```
                       POST /orders
                            |
                            v
                   +-----------------+
                   |  Order Service  |  writes Order + OrderCreated
                   +-----------------+  in one DB transaction (outbox)
                            |
                   order.events (Kafka)
                            |
                            v
                   +------------------+
                   | Payment Service  |  charges Stripe, emits
                   +------------------+  PaymentSucceeded / PaymentFailed
                            |
                  payment.events (Kafka)
                            |
              +-------------+--------------+
              |                            |
              v                            v
   +--------------------+        (PaymentFailed)
   | Inventory Service  |        -> Order Service cancels order
   +--------------------+
   reserves stock with
   row-level locking, emits
   InventoryReserved / InventoryFailed
              |
   inventory.events (Kafka)
              |
   +----------+-----------------------------+
   |                                         |
   v                                         v
InventoryReserved                     InventoryFailed
-> Order Service confirms             -> Payment Service refunds (compensation)
                                      -> Order Service cancels order

Notification Service consumes payment.events + inventory.events for confirmations.
```

Each service owns its own PostgreSQL database and subscribes to Kafka as its own
consumer group, so every service independently receives every event it cares about.

## Distributed-systems patterns

**Transactional outbox.** When a service changes business data and needs to publish
an event, it writes the event into an `outbox` table inside the same database
transaction as the business write. A background relay polls the outbox and publishes
to Kafka, then marks the row as sent. This removes the dual-write problem: an event
is published if and only if its transaction committed.

**Choreography-based saga with compensating transactions.** There is no distributed
transaction across services. Each step reacts to the previous step's event, and any
failure triggers a compensating action. If inventory cannot be reserved after a
payment has already succeeded, the payment service automatically issues a refund and
the order is cancelled.

**Idempotent consumers.** Kafka delivers at least once, so events can arrive more
than once. Consumers track processed event IDs in Redis and skip duplicates. As a
second layer, all Stripe charge and refund calls use idempotency keys, so even a
re-run of a handler cannot double-charge or double-refund.

**Row-level locking.** Inventory reservation uses `SELECT ... FOR UPDATE` to lock the
stock row for the duration of the transaction, preventing oversell under concurrent
orders for the same product.

**Dead letter queue.** An event that keeps failing is retried up to three times with
backoff, then routed to a `<topic>.dlq` topic with structured logging, so one poison
message never blocks the pipeline.

## Tech stack

| Concern            | Choice                                    |
|--------------------|-------------------------------------------|
| Services / API     | Python, FastAPI, asyncio                  |
| Event bus          | Apache Kafka (Redpanda locally)           |
| Databases          | PostgreSQL (one per service), SQLAlchemy async |
| Caching / dedup    | Redis                                     |
| Payments           | Stripe (test mode)                        |
| Packaging          | Docker, Docker Compose                    |

## Project structure

```
event-driven-payments/
├── docker-compose.yml          # Redpanda, PostgreSQL, Redis
├── init-db.sql                 # creates one database per service
├── load_test.py                # concurrent load test + latency report
├── common/                     # shared: events, db, outbox, idempotency, kafka I/O
├── order_service/
├── payment_service/
├── inventory_service/
└── notification_service/
```

## Running locally

**Prerequisites:** Docker, Python 3.11+, and a Stripe test secret key
(`sk_test_...`). Stripe can be skipped entirely by running in mock-payment mode (see
below).

1. Start the infrastructure:

```bash
   docker compose up -d
```

2. Create a virtual environment and install dependencies:

```bash
   python -m venv .venv
   source .venv/bin/activate        # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
```

3. Add a `.env` file in the project root:

```
   KAFKA_BOOTSTRAP=localhost:9092
   REDIS_URL=redis://localhost:6379/0
   STRIPE_API_KEY=sk_test_your_key_here
   ORDER_DB_URL=postgresql+asyncpg://app:app@localhost:5432/order_db
   PAYMENT_DB_URL=postgresql+asyncpg://app:app@localhost:5432/payment_db
   INVENTORY_DB_URL=postgresql+asyncpg://app:app@localhost:5432/inventory_db
   MOCK_PAYMENTS=false
```

4. Run each service in its own terminal:

```bash
   uvicorn order_service.main:app --port 8001
   uvicorn payment_service.main:app --port 8002
   uvicorn inventory_service.main:app --port 8003
   uvicorn notification_service.main:app --port 8004
```

## Trying the three flows

**Happy path** (in stock, valid card) resolves to `CONFIRMED`:

```bash
curl -X POST localhost:8001/orders -H "Content-Type: application/json" \
  -d '{"customer_email":"a@b.com","product_id":"widget-001","quantity":1,"amount_cents":2500}'
```

Check status with `curl localhost:8001/orders/<order_id>`.

**Payment failure** (declined card) resolves to `CANCELLED` without touching
inventory:

```bash
curl -X POST localhost:8001/orders -H "Content-Type: application/json" \
  -d '{"customer_email":"a@b.com","product_id":"widget-001","quantity":1,"amount_cents":2500,"simulate_decline":true}'
```

**Inventory failure with automatic refund** (order more than is in stock) succeeds at
payment, fails at inventory, and is automatically refunded and cancelled:

```bash
curl -X POST localhost:8001/orders -H "Content-Type: application/json" \
  -d '{"customer_email":"a@b.com","product_id":"widget-001","quantity":10,"amount_cents":9900}'
```

Watch the events cross the bus:

```bash
docker exec -it redpanda rpk topic consume payment.events --num 10
```

## Load test results

The system was load tested with 200 concurrent orders. Payment calls were mocked
(`MOCK_PAYMENTS=true`) to isolate the system's own capacity from Stripe's sandbox
rate limits.

- 200 of 200 orders resolved to a terminal state, with zero timeouts and zero stuck
  or duplicate transactions.
- Saga completion latency, measured from order creation to terminal status:
  - p50: 6.65s
  - p95: 7.92s
  - max: 9.09s

Run it yourself with `python load_test.py` while all services are up.

## Engineering notes

An early load test showed a p50 saga completion of ~35s with most orders timing out.
The cause was committing the Kafka offset after every individual message, a blocking
broker round trip that serialized each consumer and compounded across the saga's
multiple hops. Switching to interval-based auto-commit dropped p50 to under 7s. This
is safe specifically because the consumers are idempotent: the small reprocessing
window that auto-commit allows on crash is absorbed by Redis deduplication and Stripe
idempotency keys. The change trades a tiny, harmless reprocessing risk for a large
throughput gain.

## Roadmap

- Distributed tracing with OpenTelemetry and Jaeger, propagating trace context across
  the Kafka boundary to view a single order as one connected span timeline.
- Metrics with Prometheus and Grafana (order, payment, and DLQ counters; saga latency
  histograms).
- gRPC for a synchronous internal call alongside the asynchronous events.