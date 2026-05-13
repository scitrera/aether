#!/bin/bash
# Wrapper script for running migrations
# This allows 'go run migrations/runner.go --dry-run' style invocations
# by redirecting to the actual cmd/migrate command

cd "$(dirname "$0")/.." || exit 1
exec go run ./cmd/migrate "$@"
