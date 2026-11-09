# kafka-demo

A hands-on **Kafka learning playground**: two Spring Boot apps around a local Kafka broker, so you
can send an API request, watch it flow through Kafka, and inspect what the consumer received.

- **producer** (`:8081`) — REST `POST /api/publish` → publishes a keyed event to Kafka.
- **consumer** (`:8082`) — `@KafkaListener` consumes events into an in-memory store exposed over
  REST (`GET /api/messages`), and routes failures to a dead-letter topic (`GET /api/dlt`).
- **common** — the shared `OrderEvent` message and topic names, so the two apps can't drift.
- **Kafka** — a single **KRaft** broker (no ZooKeeper) in Docker.

Built to *practice* Kafka theory: keys/partitions, consumer groups, idempotent producer, offsets,
and dead-letter handling — all observable through plain HTTP.

## Run the whole thing (e2e)

```bash
bash scripts/dev-local.sh up        # builds the app images, starts Kafka + both apps, waits healthy
```

Then drive it:

```bash
bash scripts/dev-local.sh produce   # POST a sample event (key=customer-42)
bash scripts/dev-local.sh list      # GET what the consumer has consumed
bash scripts/dev-local.sh boom      # POST a failing event (payload=boom) → dead-letter topic
bash scripts/dev-local.sh dlt       # GET the dead-lettered events
bash scripts/dev-local.sh down      # tear down + wipe
```

Or hit the APIs directly:

```bash
curl -XPOST localhost:8081/api/publish -H 'Content-Type: application/json' \
     -d '{"key":"customer-42","type":"ORDER_PLACED","payload":"2 widgets"}'
curl localhost:8082/api/messages
curl 'localhost:8082/api/messages?key=customer-42'
```

## The message

```json
{ "eventId": "uuid", "key": "customer-42", "type": "ORDER_PLACED", "payload": "...", "createdAt": "..." }
```

`key` is the **Kafka record key** — records with the same key land on the same partition, so you can
watch per-key ordering. Two special payloads trigger the failure paths:
- `"boom"` — the consumer always throws → after retries the record is routed to the dead-letter
  topic (`GET /api/dlt`), a **permanent** failure.
- `"flaky"` — the consumer throws only on the first delivery of that event, then succeeds on retry →
  a **transient** failure that **recovers** and lands in `/api/messages` (not the DLT).

## What to try (the Kafka lessons)

- **Partitioning / ordering** — publish several events with the same `key`; they go to one partition
  in order. Different keys spread across the 3 partitions.
- **Consumer group** — run a second consumer instance (`app.consumer.group-id` is shared); watch the
  partitions get split between the two (rebalancing).
- **Idempotent producer** — the producer uses `acks=all` + `enable.idempotence=true` (no duplicates
  on retry).
- **Dead-letter topic (permanent failure)** — a `"boom"` payload is retried, then routed to
  `kafka-demo.events.DLT` and visible at `GET /api/dlt`.
- **Transient recovery** — a `"flaky"` payload fails once then succeeds on retry; it ends up in
  `/api/messages`, demonstrating that not every failure is dead-lettered.
- **Poison pill** — a malformed record (bad bytes on the topic) can't be deserialized. The consumer
  uses `ErrorHandlingDeserializer`, so it does **not** wedge the partition: the bad record is skipped
  to the DLT topic and later valid records keep flowing. A true poison pill stays as raw bytes on the
  DLT topic (it can't deserialize into the `/api/dlt` view) — inspect it on the topic directly.

> **Note:** the consumer's store is **in-memory and non-durable** — restart the consumer and it's
> empty. That's intentional for a learning tool; it is not a storage design.

## Build & test

```bash
source env.sh
mvn -B clean verify     # unit + MockMvc + Testcontainers (real Kafka broker) integration tests
```

## Run the apps from your IDE (while studying)

`dev-local.sh up` runs everything in Docker. To debug an app from your IDE instead, start just Kafka
(`docker compose up -d kafka`) and run `ProducerApplication` / `ConsumerApplication` with
`KAFKA_BOOTSTRAP_SERVERS=localhost:9092` (the default).

## Configuration (env overrides)

Nothing is hardcoded — the compose stack reads these (defaults in parentheses):

- `PRODUCER_PORT` (`8081`) / `CONSUMER_PORT` (`8082`) — host ports, if those are taken on your
  machine: `CONSUMER_PORT=18082 bash scripts/dev-local.sh up`.
- `APP_KAFKA_BOOTSTRAP` (`kafka:19092`) — the broker address the app containers connect to.
- `KAFKA_LISTENERS` / `KAFKA_ADVERTISED_LISTENERS` / `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` /
  `KAFKA_CONTROLLER_LISTENER_NAMES` / `KAFKA_INTER_BROKER_LISTENER_NAME` — broker listener wiring.
- `KAFKA_IMAGE`, `REGISTRY` — image sources (e.g. a mirror).

Cold-start note: the broker healthcheck waits for the cluster API (coordinator ready) before the
apps start, and the consumer refreshes metadata frequently, so a fresh `up` reliably assigns the
consumer all partitions rather than getting a stuck partial assignment.

## Next experiments (deliberately not in v1)

- **Kafka Streams** — stateful stream processing (aggregations, joins, windowing) declared as a
  topology, versus the low-level produce/consume this repo uses. The natural next step once the
  basics click.
- **Schema Registry + Avro** — schema-validated messages instead of JSON.
- **Manual offset commit** — commit after processing for at-least-once control.
