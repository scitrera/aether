#!/usr/bin/env bash
#
# Build all components in the Aether OSS repository.
# Does not build Docker containers — use `docker build` for that.
#
# Usage:
#   ./scripts/build-all.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

PASS=0
FAIL=0

step() {
    echo ""
    echo "=== $1 ==="
}

ok() {
    echo "  OK"
    PASS=$((PASS + 1))
}

fail() {
    echo "  FAILED"
    FAIL=$((FAIL + 1))
}

# --- Locate Go ---
if command -v go &>/dev/null; then
    GO=go
elif [ -x "${GOROOT:-}/bin/go" ]; then
    GO="$GOROOT/bin/go"
else
    # Search common install locations
    for candidate in /usr/local/go/bin/go "$HOME/sdk"/go*/bin/go "$HOME/go/bin/go"; do
        if [ -x "$candidate" ]; then
            GO="$candidate"
            break
        fi
    done
fi

if [ -z "${GO:-}" ]; then
    echo "WARNING: go not found — skipping Go builds"
    echo "  Set GOROOT or add go to PATH"
    GO=""
fi

# --- Go: API module ---
if [ -n "$GO" ]; then
    step "Go: api module"
    (cd "$REPO_ROOT/api" && "$GO" build ./...) && ok || fail

    # --- Go: SDK module ---
    step "Go: sdk/go module"
    (cd "$REPO_ROOT/sdk/go" && "$GO" build ./...) && ok || fail

    # --- Go: Server binaries ---
    step "Go: server binaries"
    SERVER_CMDS=(gateway aetherlite auth-proxy cleanup migrate init-secrets workflow msgbridge)
    SERVER_FAIL=0
    for cmd in "${SERVER_CMDS[@]}"; do
        echo "  building $cmd..."
        (cd "$REPO_ROOT/server" && "$GO" build -o "$cmd" "./cmd/$cmd") || { fail; SERVER_FAIL=1; }
    done
    [ "$SERVER_FAIL" -eq 0 ] && ok
fi

# --- Python SDK ---
step "Python: sdk/python-client (syntax check)"
if command -v python3 &>/dev/null; then
    python3 -m py_compile "$REPO_ROOT/sdk/python-client/scitrera_aether_client/__init__.py" && ok || fail
else
    echo "  SKIPPED (python3 not found)"
fi

# --- TypeScript SDK ---
step "TypeScript: sdk/typescript"
if command -v npm &>/dev/null; then
    if [ -d "$REPO_ROOT/sdk/typescript/node_modules" ]; then
        (cd "$REPO_ROOT/sdk/typescript" && npm run build) && ok || fail
    else
        echo "  node_modules not found — running npm install first..."
        (cd "$REPO_ROOT/sdk/typescript" && npm install && npm run build) && ok || fail
    fi
else
    echo "  SKIPPED (npm not found)"
fi

# --- Summary ---
echo ""
echo "==============================="
echo "Build complete: $PASS passed, $FAIL failed"
echo "==============================="

exit "$FAIL"
