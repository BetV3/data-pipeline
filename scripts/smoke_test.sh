#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

wait_healthy() {
  local name="$1"
  local timeout="${2:-120}"
  local start
  start="$(date +%s)"
  echo "==> Waiting for ${name} to be healthy..."
  while true; do
    local status=""
    status="$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null || true)"
    if [ "$status" = "healthy" ]; then
      echo "==> ${name} is healthy"
      return 0
    fi
    local now
    now="$(date +%s)"
    if [ $((now - start)) -ge "$timeout" ]; then
      echo "❌ Timeout waiting for ${name} to be healthy (last status: '${status}')"
      exit 1
    fi
    sleep 2
  done
}

ensure_venv() {
  if [ ! -x ".venv/bin/python" ]; then
    rm -rf .venv
    python3 -m venv .venv
  fi
}

echo "==> Starting platform stack"
cd "$ROOT_DIR/platform/compose"
docker compose up -d

wait_healthy "dpp-postgres" 120
wait_healthy "dpp-kafka" 180

echo "==> Waiting for topic init to finish"
docker wait dpp-kafka-init >/dev/null 2>&1 || true
INIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' dpp-kafka-init 2>/dev/null || echo 1)"
if [ "$INIT_CODE" != "0" ]; then
  echo "❌ kafka-init failed (exit=$INIT_CODE)"
  exit 1
fi

echo "==> Starting consumer (background)"
(
  cd "$ROOT_DIR/apps/consumer-python"
  ensure_venv
  .venv/bin/pip install -q -r requirements.txt
  exec .venv/bin/python src/consumer.py --poll-ms 500
) &>/dev/null &
CONSUMER_PID=$!

echo "==> Running producer"
(
  cd "$ROOT_DIR/apps/producer-python"
  ensure_venv
  .venv/bin/pip install -q -r requirements.txt
  .venv/bin/python src/producer.py --seconds 3 --rate 20
)

echo "==> Waiting for consumer to write rows"
sleep 2

echo "==> Checking Postgres row count"
COUNT="$(docker exec -i dpp-postgres psql -U platform -d platform -t -c "SELECT COUNT(*) FROM raw_events;" | tr -d '[:space:]')"
echo "raw_events count: $COUNT"

echo "==> Cleaning up"
kill "$CONSUMER_PID" >/dev/null 2>&1 || true

if [ "${COUNT:-0}" -gt 0 ]; then
  echo "✅ Smoke test passed"
  exit 0
else
  echo "❌ Smoke test failed (no rows written)"
  exit 1
fi