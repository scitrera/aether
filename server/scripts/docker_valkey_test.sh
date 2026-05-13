#!/usr/bin/env bash
# Bring up an ephemeral Valkey (Redis-compatible) instance for local
# development. Single-node mode; the gateway dev config (configs/dev.yaml)
# expects port 56379.
#
# Usage:
#   bash server/scripts/docker_valkey_test.sh
#
set -euo pipefail

IMAGE="valkey/valkey:latest"
PORT="${PORT:-56379}"
PASSWORD="${PASSWORD:-}"

CMD=(valkey-server
  --bind 0.0.0.0
  --protected-mode no
  --appendonly no
  --save ""
)

if [[ -n "$PASSWORD" ]]; then
  CMD+=( --requirepass "$PASSWORD" )
fi

echo ""
echo "Starting Valkey (ephemeral)..."
echo "   localhost:${PORT}"
[[ -n "$PASSWORD" ]] && echo "   Password: ${PASSWORD}"
echo ""

docker run -it --rm --name valkey \
  -p "${PORT}:6379" \
  "$IMAGE" "${CMD[@]}"
