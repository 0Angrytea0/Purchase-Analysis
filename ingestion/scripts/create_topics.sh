#!/usr/bin/env bash
set -euo pipefail

TOPIC="purchases"
BOOTSTRAP="kafka:9092"

if kafka-topics --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC"; then
  echo "✔ Topic $TOPIC already exists"
else
  echo "→ Creating topic $TOPIC"
  kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1
  echo "✔ Created $TOPIC"
fi
