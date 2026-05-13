#!/usr/bin/env bash
# Bring up an ephemeral RabbitMQ Streams broker for local development.
#
# Ports:
#   55552  — RabbitMQ Streams protocol (advertised at 127.0.0.1)
#   55672  — AMQP 0-9-1 protocol
#   15672  — Management UI / HTTP API (guest:guest)
#
# Usage:
#   bash server/scripts/docker_rmq_test.sh
#
# The container is removed automatically on Ctrl-C.
set -euo pipefail

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

cat > "$TMPDIR/rabbitmq.conf" <<'EOF'
loopback_users = none
stream.advertised_host = 127.0.0.1
stream.advertised_port = 55552
EOF

docker run -it --rm --name rabbitmq \
  -p 55552:5552 -p 55672:5672 -p 15672:15672 \
  -v "$TMPDIR/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro" \
  ghcr.io/scitrera/rabbitmq-stream:4-management
