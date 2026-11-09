---
description: kafka-demo usage — a Kafka produce→consume→inspect learning playground; how to run it and the structure to follow
alwaysApply: true
---

This repo's AI onboarding and usage policy lives in one place: **[AGENTS.md](../../AGENTS.md)**.

Read it first. kafka-demo is a Kafka learning scaffold (producer `:8081` → KRaft Kafka → consumer
`:8082`, inspect over REST). Run the full stack with `bash scripts/dev-local.sh up`. Keep changes
small, match the `common`/`producer`/`consumer` structure, keep `mvn clean verify` green.
