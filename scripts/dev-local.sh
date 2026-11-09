#!/bin/bash
# Full local e2e loop for kafka-demo: KRaft Kafka + producer + consumer as containers.
#
#   bash scripts/dev-local.sh up        # build app images + compose up + wait for health
#   bash scripts/dev-local.sh down      # tear everything down
#   bash scripts/dev-local.sh logs      # follow all logs
#   bash scripts/dev-local.sh produce   # publish a sample event (key=customer-42)
#   bash scripts/dev-local.sh boom      # publish a failing event (payload=boom -> DLT)
#   bash scripts/dev-local.sh list      # GET consumed messages
#   bash scripts/dev-local.sh dlt       # GET dead-lettered messages
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MIRROR="${MIRROR:-armdocker.rnd.ericsson.se/dockerhub-ericsson-remote}"
export REGISTRY="${REGISTRY:-$MIRROR/library}"
export KAFKA_IMAGE="${KAFKA_IMAGE:-$MIRROR/apache/kafka:3.8.0}"
PRODUCER="http://localhost:8081"
CONSUMER="http://localhost:8082"

build() {
  local secret=()
  [ -f "$HOME/.m2/settings.xml" ] && secret=(--secret id=m2settings,src="$HOME/.m2/settings.xml")
  echo "== build app images (registry: $REGISTRY) =="
  ( cd "$REPO" && DOCKER_BUILDKIT=1 docker build "${secret[@]}" \
      --build-arg REGISTRY="$REGISTRY" -f producer/Dockerfile -t kafka-demo-producer:0.1.0 . )
  ( cd "$REPO" && DOCKER_BUILDKIT=1 docker build "${secret[@]}" \
      --build-arg REGISTRY="$REGISTRY" -f consumer/Dockerfile -t kafka-demo-consumer:0.1.0 . )
}

wait_health() {
  local name="$1" base="$2"
  echo "== waiting for $name health =="
  for _ in $(seq 1 60); do
    [ "$(curl -sS -m 3 -o /dev/null -w '%{http_code}' "$base/actuator/health" 2>/dev/null)" = "200" ] \
      && { echo "UP -> $base"; return 0; }
    sleep 3
  done
  echo "$name did not become healthy"; ( cd "$REPO" && docker compose logs "$name" | tail -40 ); return 1
}

up() {
  build
  echo "== compose up (kafka + producer + consumer) =="
  ( cd "$REPO" && docker compose up -d )
  wait_health producer "$PRODUCER" && wait_health consumer "$CONSUMER"
  echo
  echo "Ready. Try:"
  echo "  bash scripts/dev-local.sh produce   # publish an event"
  echo "  bash scripts/dev-local.sh list      # see it consumed"
}

down() { ( cd "$REPO" && docker compose down -v --remove-orphans ); }
logs() { ( cd "$REPO" && docker compose logs -f ); }

produce() {
  curl -fsS -XPOST "$PRODUCER/api/publish" -H 'Content-Type: application/json' \
       -d '{"key":"customer-42","type":"ORDER_PLACED","payload":"2 widgets"}' && echo
}

boom() {
  curl -fsS -XPOST "$PRODUCER/api/publish" -H 'Content-Type: application/json' \
       -d '{"key":"customer-42","type":"ORDER_PLACED","payload":"boom"}' && echo
}

list() { curl -fsS "$CONSUMER/api/messages" && echo; }
dlt()  { curl -fsS "$CONSUMER/api/dlt" && echo; }

case "${1:-}" in
  up)      up ;;
  down)    down ;;
  logs)    logs ;;
  produce) produce ;;
  boom)    boom ;;
  list)    list ;;
  dlt)     dlt ;;
  *) echo "usage: $0 {up|down|logs|produce|boom|list|dlt}"; exit 2 ;;
esac
