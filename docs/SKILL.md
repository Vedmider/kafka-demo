# Skill: kafka-demo — a Kafka produce → consume → inspect playground

Provider-agnostic guidance for an AI agent (or a human) using **kafka-demo** to practice Kafka.

## What it is

Two Spring Boot apps around a local KRaft Kafka broker:
- **producer** `:8081` — `POST /api/publish {key, type, payload}` publishes a keyed `OrderEvent`.
- **consumer** `:8082` — consumes into an in-memory store; `GET /api/messages` (`?key=`,
  `/{eventId}`), `GET /api/dlt`, `DELETE /api/messages` (reset).

Message: `OrderEvent{eventId, key, type, payload, createdAt}` (shared in `common`). The `key` is the
Kafka record key (drives partitioning). Special payloads: `"boom"` = permanent failure (→ DLT);
`"flaky"` = transient failure that recovers on retry (→ consumed, not DLT).

## Run

`bash scripts/dev-local.sh up` starts Kafka + both apps (e2e); `produce|list|boom|dlt|down` are
helper verbs. Apps read `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`). Host ports, broker
bootstrap, and listener wiring are env-overridable (`PRODUCER_PORT`/`CONSUMER_PORT`/
`APP_KAFKA_BOOTSTRAP`/`KAFKA_*`) — see README "Configuration".

## The lessons (what to actually practice)

1. **Partitioning & ordering** — publish several events with the same `key` and confirm order via
   `GET /api/messages?key=...`; different keys spread across the 3 partitions.
2. **Consumer group** — start a second consumer (same `app.consumer.group-id`); the partitions are
   divided between instances (rebalancing). Stop one; the other takes all partitions.
3. **Idempotent, durable produce** — `acks=all` + `enable.idempotence=true`.
4. **Permanent failure → DLT** — `payload="boom"` → retried (`FixedBackOff`) → published to
   `kafka-demo.events.DLT` → visible at `GET /api/dlt`, kept out of the main store.
5. **Transient failure → recovery** — `payload="flaky"` fails once then succeeds on retry; ends up
   in `/api/messages`, not the DLT.
6. **Poison pill** — malformed bytes on the topic are handled by `ErrorHandlingDeserializer`: the
   bad record is skipped to the DLT topic instead of wedging the partition, and later valid records
   keep flowing. Raw poison bytes stay on the DLT topic (can't deserialize into `/api/dlt`).

## When NOT to reach for this

- It's not a durable store (in-memory; restart loses state) and not production Kafka config
  (single broker, replication factor 1).
- It does **not** use Kafka Streams/Connect/Schema Registry — see README "Next experiments".

## Conventions

- Package base `io.github.vedmider.kafkademo`; modules `common` / `producer` / `consumer`.
- The message contract and topic names live only in `common` — change them there, never inline.
- Keep `mvn clean verify` green; the Testcontainers ITs spin a real broker and prove the flow.
