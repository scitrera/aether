#!/usr/bin/env bash
#
# Create git tags for all Go modules in the Aether monorepo.
# Reads the version from versions.yaml (aether-gateway key).
#
# Usage:
#   ./scripts/tag-release.sh            # dry-run (default)
#   ./scripts/tag-release.sh --push     # create tags and push to origin
#   ./scripts/tag-release.sh --dry-run  # explicit dry-run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
VERSIONS_FILE="$REPO_ROOT/versions.yaml"

# Defaults
DRY_RUN=true
PUSH=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --push)
            DRY_RUN=false
            PUSH=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [--push | --dry-run]"
            echo ""
            echo "  --dry-run  Show what would be done (default)"
            echo "  --push     Create tags and push to origin"
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

# Read version from versions.yaml
if [[ ! -f "$VERSIONS_FILE" ]]; then
    echo "ERROR: versions.yaml not found at $VERSIONS_FILE" >&2
    exit 1
fi

VERSION=$(grep '^aether-gateway:' "$VERSIONS_FILE" | sed 's/^aether-gateway:\s*//' | sed 's/\s*#.*//' | tr -d '[:space:]')

if [[ -z "$VERSION" ]]; then
    echo "ERROR: Could not read aether-gateway version from $VERSIONS_FILE" >&2
    exit 1
fi

# Validate semver
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
    echo "ERROR: Invalid semver: $VERSION" >&2
    exit 1
fi

echo "Version: $VERSION"
echo ""

# Check for clean working directory
cd "$REPO_ROOT"
if [[ -n "$(git status --porcelain)" ]]; then
    echo "WARNING: Working directory is not clean." >&2
    echo "Commit or stash changes before tagging a release." >&2
    if [[ "$DRY_RUN" == false ]]; then
        echo "Aborting." >&2
        exit 1
    fi
    echo "(Continuing in dry-run mode...)"
    echo ""
fi

# Define tags for Go multi-module repo
TAGS=(
    "v${VERSION}"           # root/server module: github.com/scitrera/aether
    "api/v${VERSION}"       # api module: github.com/scitrera/aether/api
    "sdk/go/v${VERSION}"    # go sdk module: github.com/scitrera/aether/sdk/go
)

# Check for existing tags
EXISTING=()
for tag in "${TAGS[@]}"; do
    if git rev-parse "$tag" &>/dev/null; then
        EXISTING+=("$tag")
    fi
done

if [[ ${#EXISTING[@]} -gt 0 ]]; then
    echo "WARNING: The following tags already exist:" >&2
    for tag in "${EXISTING[@]}"; do
        echo "  $tag" >&2
    done
    if [[ "$DRY_RUN" == false ]]; then
        echo "Aborting. Delete existing tags first if you want to re-tag." >&2
        exit 1
    fi
    echo ""
fi

# Create/display tags
if [[ "$DRY_RUN" == true ]]; then
    echo "DRY RUN -- would create the following tags:"
    for tag in "${TAGS[@]}"; do
        echo "  git tag -a $tag -m \"Release $tag\""
    done
    if [[ "$PUSH" == true ]]; then
        echo ""
        echo "  git push origin ${TAGS[*]}"
    fi
    echo ""
    echo "Run with --push to create and push tags."
else
    echo "Creating tags..."
    for tag in "${TAGS[@]}"; do
        echo "  $tag"
        git tag -a "$tag" -m "Release $tag"
    done

    if [[ "$PUSH" == true ]]; then
        echo ""
        echo "Pushing tags to origin..."
        git push origin "${TAGS[@]}"
        echo "Done."
    else
        echo ""
        echo "Tags created locally. Push with: git push origin ${TAGS[*]}"
    fi
fi
