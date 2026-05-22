#!/usr/bin/env bash
#
# Run the same checks locally that .github/workflows/test.yml runs in CI.
#
# Jobs (matching the workflow):
#   test          go vet ./...
#                 go test -short -race -count=1 -coverprofile=coverage.out -covermode=atomic ./...
#   lint          golangci-lint run --timeout=5m            (pinned to v2.11.4)
#   integration   go test -race -count=1 -tags=integration ./...
#                 with Redis (Valkey 8), RabbitMQ Streams, PostgreSQL services
#   e2e           go test -tags=e2e -count=1 -timeout 360s ./internal/proxysidecar/integration_e2e/...
#                 spawns aetherlite subprocess; no external services needed
#                 (scoped to integration_e2e dir; other no-build-tag test
#                 dirs that need NATS/JetStream are covered by integration)
#   security      govulncheck ./...                          (pinned to v1.1.4)
#
# Usage:
#   ./scripts/run-ci.sh                       # all four jobs in order
#   ./scripts/run-ci.sh test                  # one job
#   ./scripts/run-ci.sh test lint security    # any subset
#   ./scripts/run-ci.sh integration           # also spins up service containers
#
# Environment knobs:
#   GO              path to go binary (auto-detected if unset)
#   KEEP_SERVICES   if set, don't tear down integration-test containers on exit
#   SKIP_INSTALL    if set, fail if golangci-lint / govulncheck not already on PATH
#                   (default behaviour is to `go install` them at the pinned version)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Versions pinned by .github/workflows/test.yml — keep in sync.
CI_GO_VERSION="1.25.10"
CI_GOLANGCI_LINT_VERSION="v2.11.4"
CI_GOVULNCHECK_VERSION="v1.1.4"

# Integration-service images + ports (mirroring the workflow's `services:`).
REDIS_IMAGE="valkey/valkey:8-alpine"
REDIS_PORT="56379"
RMQ_IMAGE="ghcr.io/scitrera/rabbitmq-stream:4-management"
RMQ_STREAM_PORT="55552"
RMQ_AMQP_PORT="55672"
POSTGRES_IMAGE="postgres:16-alpine"
POSTGRES_PORT="5432"

CONTAINER_PREFIX="aether-ci-local"
SERVICE_CONTAINERS=(
    "${CONTAINER_PREFIX}-redis"
    "${CONTAINER_PREFIX}-rmq"
    "${CONTAINER_PREFIX}-pg"
)

# Per-job logs. Set LOG_DIR=... to override; default is a fresh timestamped dir.
LOG_DIR="${LOG_DIR:-${REPO_ROOT}/.run-ci-logs/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$LOG_DIR"

PASS=0
FAIL=0
FAILED_JOBS=()

step() {
    echo ""
    echo "=== $1 ==="
}

ok() {
    echo "  OK"
    PASS=$((PASS + 1))
}

# summarize_failure greps the per-job log for failure-context lines and prints
# them so the user doesn't have to re-run with -v or page through scrollback.
summarize_failure() {
    local job="$1"
    local log="$LOG_DIR/$job.log"
    if [ ! -s "$log" ]; then
        return 0
    fi
    echo ""
    echo "  --- failure summary for $job ---"
    # Patterns: go test individual failures, package-level fails, panics,
    # linter findings (golangci-lint emits "path:line:col: severity: msg"),
    # govulncheck vuln blocks. Cap at 50 lines so it stays scannable.
    grep -nE '^--- FAIL:|^FAIL[[:space:]]+|^panic:|^[[:space:]]*goroutine [0-9]+ \[|^[^[:space:]].*:[0-9]+:[0-9]+:[[:space:]]+(error|warning|fatal)|^Vulnerability #' "$log" \
        | head -50 || true
    echo "  --- full log: $log ---"
}

fail() {
    echo "  FAILED"
    FAIL=$((FAIL + 1))
    FAILED_JOBS+=("$1")
    summarize_failure "$1"
}

# run_logged JOB_NAME -- CMD... ARGS...
# Runs CMD with stdout+stderr teed to LOG_DIR/<job>.log. Returns CMD's exit
# code regardless of tee's exit. Caller is responsible for calling ok/fail.
run_logged() {
    local job="$1"; shift
    local log="$LOG_DIR/$job.log"
    echo "  log: $log"
    set +o pipefail
    "$@" 2>&1 | tee "$log"
    local rc=${PIPESTATUS[0]}
    set -o pipefail
    return "$rc"
}

# --- Locate Go (mirrors scripts/build-all.sh) ---
if [ -n "${GO:-}" ]; then
    : # use the override
elif command -v go &>/dev/null; then
    GO=go
elif [ -x "${GOROOT:-}/bin/go" ]; then
    GO="$GOROOT/bin/go"
else
    for candidate in /usr/local/go/bin/go "$HOME/sdk"/go*/bin/go "$HOME/go/bin/go"; do
        if [ -x "$candidate" ]; then
            GO="$candidate"
            break
        fi
    done
fi

if [ -z "${GO:-}" ] || ! command -v "$GO" &>/dev/null && [ ! -x "$GO" ]; then
    echo "ERROR: go not found. Set GO=/path/to/go, GOROOT, or add go to PATH."
    exit 2
fi

LOCAL_GO_VERSION="$("$GO" env GOVERSION 2>/dev/null | sed 's/^go//')"
if [ "$LOCAL_GO_VERSION" != "$CI_GO_VERSION" ]; then
    echo "NOTE: local go $LOCAL_GO_VERSION differs from CI go $CI_GO_VERSION (continuing)."
fi

# Make `go install`-d tools reachable in this run.
GOBIN="$("$GO" env GOBIN 2>/dev/null || true)"
if [ -z "$GOBIN" ]; then
    GOBIN="$("$GO" env GOPATH)/bin"
fi
case ":$PATH:" in
    *":$GOBIN:"*) ;;
    *) PATH="$GOBIN:$PATH" ;;
esac
export PATH

# --- Job implementations ---

job_test() {
    step "test (go vet + go test -short -race -coverprofile)"
    run_logged test bash -c "
        cd '$REPO_ROOT/server' && \
        '$GO' vet ./... && \
        '$GO' test -short -race -count=1 -v -coverprofile=coverage.out -covermode=atomic ./...
    " && ok || fail "test"
}

ensure_golangci_lint() {
    local need_install=0
    if ! command -v golangci-lint &>/dev/null; then
        need_install=1
    else
        local v
        v="$(golangci-lint version --short 2>/dev/null | head -1 | sed 's/^v//')"
        local want="${CI_GOLANGCI_LINT_VERSION#v}"
        if [ "$v" != "$want" ]; then
            echo "  golangci-lint $v on PATH, want $CI_GOLANGCI_LINT_VERSION — installing pinned version"
            need_install=1
        fi
    fi
    if [ "$need_install" -eq 1 ]; then
        if [ -n "${SKIP_INSTALL:-}" ]; then
            echo "  SKIP_INSTALL set; refusing to install golangci-lint" >&2
            return 1
        fi
        echo "  installing golangci-lint $CI_GOLANGCI_LINT_VERSION via go install"
        "$GO" install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${CI_GOLANGCI_LINT_VERSION}"
    fi
}

job_lint() {
    step "lint (golangci-lint $CI_GOLANGCI_LINT_VERSION)"
    if ! ensure_golangci_lint; then
        fail "lint"
        return 1
    fi
    run_logged lint bash -c "
        cd '$REPO_ROOT/server' && \
        golangci-lint run --timeout=5m ./...
    " && ok || fail "lint"
}

ensure_docker() {
    if ! command -v docker &>/dev/null; then
        echo "  ERROR: docker not found; integration tests need Redis/RMQ/Postgres" >&2
        return 1
    fi
    if ! docker info &>/dev/null; then
        echo "  ERROR: docker daemon not reachable" >&2
        return 1
    fi
}

teardown_services() {
    if [ -n "${KEEP_SERVICES:-}" ]; then
        echo "  KEEP_SERVICES set; leaving containers running:"
        for c in "${SERVICE_CONTAINERS[@]}"; do
            echo "    $c"
        done
        return 0
    fi
    echo "  tearing down service containers..."
    for c in "${SERVICE_CONTAINERS[@]}"; do
        docker rm -f "$c" &>/dev/null || true
    done
}

wait_for_tcp() {
    local host="$1" port="$2" name="$3" timeout="${4:-60}"
    local i=0
    while [ $i -lt "$timeout" ]; do
        if (echo >"/dev/tcp/$host/$port") &>/dev/null; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    echo "  ERROR: $name did not become reachable at $host:$port within ${timeout}s" >&2
    return 1
}

start_services() {
    # Clean any stale containers from a prior aborted run.
    for c in "${SERVICE_CONTAINERS[@]}"; do
        docker rm -f "$c" &>/dev/null || true
    done

    echo "  starting redis (valkey)..."
    docker run -d --rm --name "${CONTAINER_PREFIX}-redis" \
        -p "${REDIS_PORT}:6379" \
        "$REDIS_IMAGE" >/dev/null

    echo "  starting rabbitmq-stream..."
    docker run -d --rm --name "${CONTAINER_PREFIX}-rmq" \
        -p "${RMQ_STREAM_PORT}:5552" \
        -p "${RMQ_AMQP_PORT}:5672" \
        -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host "127.0.0.1" -rabbitmq_stream advertised_port 55552' \
        "$RMQ_IMAGE" >/dev/null

    echo "  starting postgres..."
    docker run -d --rm --name "${CONTAINER_PREFIX}-pg" \
        -p "${POSTGRES_PORT}:5432" \
        -e POSTGRES_USER=aether \
        -e POSTGRES_PASSWORD=aether_test \
        -e POSTGRES_DB=aether \
        "$POSTGRES_IMAGE" >/dev/null

    wait_for_tcp 127.0.0.1 "$REDIS_PORT" redis 30
    wait_for_tcp 127.0.0.1 "$POSTGRES_PORT" postgres 30
    # RMQ + streams plugin is the slow one — give it longer.
    wait_for_tcp 127.0.0.1 "$RMQ_AMQP_PORT" "rabbitmq AMQP" 90
    wait_for_tcp 127.0.0.1 "$RMQ_STREAM_PORT" "rabbitmq streams" 90
}

job_integration() {
    step "integration (go test -tags=integration with service containers)"
    if ! ensure_docker; then
        fail "integration"
        return 1
    fi
    start_services
    trap teardown_services EXIT
    run_logged integration bash -c "
        cd '$REPO_ROOT/server' && \
        REDIS_ADDR='localhost:${REDIS_PORT}' \
        RABBITMQ_STREAM_URL='rabbitmq-stream://guest:guest@localhost:${RMQ_STREAM_PORT}' \
        RABBITMQ_AMQP_URL='amqp://guest:guest@localhost:${RMQ_AMQP_PORT}/' \
        POSTGRES_DSN='postgres://aether:aether_test@localhost:${POSTGRES_PORT}/aether?sslmode=disable' \
            '$GO' test -race -count=1 -v -tags=integration ./...
    " && ok || fail "integration"
    teardown_services
    trap - EXIT
}

job_e2e() {
    step "e2e (go test -tags=e2e ./internal/proxysidecar/integration_e2e/...)"
    run_logged e2e bash -c "
        cd '$REPO_ROOT/server' && \
        AETHER_ALLOW_DEV_MODE=true \
            '$GO' test -tags=e2e -count=1 -v -timeout 360s ./internal/proxysidecar/integration_e2e/...
    " && ok || fail "e2e"
}

ensure_govulncheck() {
    if ! command -v govulncheck &>/dev/null; then
        if [ -n "${SKIP_INSTALL:-}" ]; then
            echo "  SKIP_INSTALL set; refusing to install govulncheck" >&2
            return 1
        fi
        echo "  installing govulncheck $CI_GOVULNCHECK_VERSION via go install"
        "$GO" install "golang.org/x/vuln/cmd/govulncheck@${CI_GOVULNCHECK_VERSION}"
    fi
}

job_security() {
    step "security (govulncheck $CI_GOVULNCHECK_VERSION)"
    if ! ensure_govulncheck; then
        fail "security"
        return 1
    fi
    run_logged security bash -c "
        cd '$REPO_ROOT/server' && \
        govulncheck ./...
    " && ok || fail "security"
}

# --- Driver ---

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

ALL_JOBS=(test lint integration e2e security)
SELECTED_JOBS=()

if [ $# -eq 0 ]; then
    SELECTED_JOBS=("${ALL_JOBS[@]}")
else
    for arg in "$@"; do
        case "$arg" in
            -h|--help) usage ;;
            test|lint|integration|e2e|security) SELECTED_JOBS+=("$arg") ;;
            all) SELECTED_JOBS=("${ALL_JOBS[@]}") ;;
            *) echo "unknown job: $arg (valid: ${ALL_JOBS[*]} all)" >&2; exit 2 ;;
        esac
    done
fi

echo "running jobs: ${SELECTED_JOBS[*]}"
echo "using GO=$GO ($("$GO" version | awk '{print $3}'))"
echo "logs: $LOG_DIR"

for job in "${SELECTED_JOBS[@]}"; do
    case "$job" in
        test) job_test || true ;;
        lint) job_lint || true ;;
        integration) job_integration || true ;;
        e2e) job_e2e || true ;;
        security) job_security || true ;;
    esac
done

echo ""
echo "==============================="
echo "CI mirror complete: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
    echo "failed jobs: ${FAILED_JOBS[*]}"
fi
echo "==============================="

[ "$FAIL" -eq 0 ]
