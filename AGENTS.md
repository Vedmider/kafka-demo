# AGENTS.md — start here (any AI)

Single entry point for an AI agent working in **kafka-demo**. AI-agnostic (Claude, Cursor, Kiro,
Codex, …). Read this, then follow the policy it points at. Nothing here is a second copy of the docs.

## What this repo is (30 seconds)

A **Kafka learning playground**, not a product: two Spring Boot apps around a local KRaft Kafka
broker, built so a human can send an HTTP request and *watch* it flow through Kafka end to end.

- **producer** (`:8081`) — `POST /api/publish {key, type, payload}` → publishes a keyed `OrderEvent`.
- **consumer** (`:8082`) — consumes into an in-memory store exposed at `GET /api/messages`
  (`?key=` filter, `/{eventId}` lookup), with failures routed to a dead-letter topic (`GET /api/dlt`).
- **common** — shared `OrderEvent` + topic names.
- Multi-module Maven; package base `io.github.vedmider.kafkademo`.

## How to run it (full e2e)

```bash
bash scripts/dev-local.sh up        # Kafka + both apps in Docker, waits for health
bash scripts/dev-local.sh produce   # publish a sample event
bash scripts/dev-local.sh list      # see it consumed
bash scripts/dev-local.sh boom      # publish a failing event → DLT
bash scripts/dev-local.sh dlt       # inspect dead-letters
bash scripts/dev-local.sh down
```

Build/test: `source env.sh && mvn -B clean verify` (unit + MockMvc + Testcontainers real-Kafka ITs).

## What it demonstrates (and what it deliberately doesn't)

The point is Kafka *mechanics*, made observable over HTTP:
- **keys → partitioning/ordering** (same key → same partition; topic has 3 partitions),
- **consumer group** (run a 2nd consumer, watch rebalancing),
- **idempotent producer** (`acks=all`, `enable.idempotence=true`),
- **dead-letter topic** (`payload="boom"` → retried → `kafka-demo.events.DLT`).

It uses the **low-level producer/consumer client on purpose**. It does **not** use Kafka Streams —
that's a higher-level stream-processing library (stateful aggregations/joins/windowing declared as a
topology) that would hide the very mechanics this repo exists to teach. Kafka Streams, Kafka Connect,
and Schema Registry/Avro are noted in the README as follow-on experiments, not part of v1.

Honest caveat: the consumer store is **in-memory, non-durable** — restart = empty. Intentional for a
learning tool.

## Policy for working in this repo

- It's a learning scaffold: keep changes small and legible, prefer clarity over cleverness.
- Match the existing structure (`common`/`producer`/`consumer`), the message contract in `common`,
  and the `io.github.vedmider.kafkademo` package base.
- Keep `mvn clean verify` green — the Testcontainers ITs are the real proof the flow works.
- Full detail: [`docs/SKILL.md`](docs/SKILL.md) and [`README.md`](README.md).

## Optional hard enforcement (Claude Code)

A `PreToolUse` speed-bump that asks before a native `Grep`/`Glob` (nudging you to read the small,
well-structured source first) is available opt-in at [`hooks/claude/install.md`](hooks/claude/install.md).
It asks, never blocks; Claude-specific. Every other client still gets this policy via `AGENTS.md`.
