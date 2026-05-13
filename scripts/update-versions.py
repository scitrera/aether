#!/usr/bin/env python3
"""
Synchronize version numbers across all Aether OSS subprojects.

Reads versions.yaml as the single source of truth and updates:
  - pyproject.toml    (Python packages)
  - package.json      (TypeScript/Node packages)
  - __init__.py       (Python runtime __version__)
  - version.go        (Go version constants)
  - go.mod            (sibling-module require pins inside the monorepo)

Usage:
    python scripts/update-versions.py           # from repo root
    python scripts/update-versions.py --check   # dry-run: exit 1 if out of sync
    python scripts/update-versions.py --verbose # show every file touched
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit(
        "PyYAML is required.  Install it with:\n"
        "  pip install pyyaml\n"
        "  uv pip install pyyaml"
    )

logger = logging.getLogger("update-versions")

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent  # scripts/
OSS_ROOT = SCRIPT_DIR.parent  # repo root
VERSIONS_YAML = OSS_ROOT / "versions.yaml"

# ---------------------------------------------------------------------------
# Per-project update rules
#
# Each entry maps a versions.yaml key to the files that must be updated and
# the strategy used.  Adding a new subproject only requires a new entry here.
# ---------------------------------------------------------------------------

# Strategies:
#   pyproject      – rewrite `version = "X.Y.Z"` in [project] table
#   package        – rewrite `"version": "X.Y.Z"` in JSON
#   init_py        – rewrite `__version__ = "X.Y.Z"` in a Python file
#   go_version     – rewrite `const Version = "X.Y.Z"` in a Go file
#   gomod_require  – rewrite `<module_path> v<version>` inside a go.mod require
#                    block.  This strategy takes a 3rd tuple element: the full
#                    target module path (e.g. "github.com/scitrera/aether/api").

# Rules are tuples: (strategy, rel_path) or (strategy, rel_path, extra_arg).
UpdateRule = tuple

PROJECT_RULES: dict[str, list[UpdateRule]] = {
    "aether-gateway": [
        ("go_version", Path("server/internal/version/version.go")),
        # Keep server/go.mod's require pins for sibling monorepo modules in
        # lockstep with this version.  The `replace` directives in go.mod
        # handle local builds, but downstream `go get` consumers see these
        # pins, so they must reference real published tags.
        ("gomod_require", Path("server/go.mod"), "github.com/scitrera/aether/api"),
        ("gomod_require", Path("server/go.mod"), "github.com/scitrera/aether/sdk/go"),
    ],
    "aether-sdk-go": [
        # Go module versions are git-tag driven; this rule only keeps the
        # sibling-module require version (api → sdk/go) in sync so that
        # transitive consumers via pkg.go.dev resolve correctly.
        ("gomod_require", Path("sdk/go/go.mod"), "github.com/scitrera/aether/api"),
    ],
    "aether-sdk-python": [
        ("pyproject", Path("sdk/python-client/pyproject.toml")),
        ("init_py", Path("sdk/python-client/scitrera_aether_client/__init__.py")),
    ],
    "aether-sdk-typescript": [
        ("package", Path("sdk/typescript/package.json")),
    ],
}

# ---------------------------------------------------------------------------
# Updaters — each returns (changed: bool, old_version: str | None)
# ---------------------------------------------------------------------------

# Matches: version = "1.2.3"  (with optional surrounding whitespace)
_RE_PYPROJECT_VERSION = re.compile(
    r'^(\s*version\s*=\s*")[^"]*(")', re.MULTILINE
)

# Matches: __version__ = "1.2.3"  (single or double quotes)
_RE_INIT_VERSION = re.compile(
    r'''^(\s*__version__\s*=\s*['"])[^'"]*(['"])''', re.MULTILINE
)

# Matches: const Version = "1.2.3"
_RE_GO_VERSION = re.compile(
    r'^(\s*const\s+Version\s*=\s*")[^"]*(")', re.MULTILINE
)


def _make_gomod_re(module_path: str) -> re.Pattern:
    """
    Build a regex that captures the version token on a go.mod require line for
    the given module path.  Matches lines inside a `require ( ... )` block as
    well as bare `require <module> v...` lines.

    Capture groups:
      1: the prefix (leading whitespace, optional `require ` keyword, module
         path, separating whitespace, and the `v`)
      2: the version token following `v` (without the leading `v`)
    """
    escaped = re.escape(module_path)
    return re.compile(
        rf'(^[ \t]*(?:require[ \t]+)?{escaped}[ \t]+v)(\S+)',
        re.MULTILINE,
    )


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def update_pyproject(path: Path, version: str, dry_run: bool) -> tuple[bool, str | None]:
    """Update version = "..." in a pyproject.toml."""
    text = _read_text(path)
    m = _RE_PYPROJECT_VERSION.search(text)
    if not m:
        logger.warning("No version field found in %s", path)
        return False, None

    old = text[m.start(1) + len(m.group(1)):m.end(2) - len(m.group(2))]
    if old == version:
        return False, old

    new_text = _RE_PYPROJECT_VERSION.sub(rf"\g<1>{version}\2", text, count=1)
    if not dry_run:
        _write_text(path, new_text)
    return True, old


def update_init_py(path: Path, version: str, dry_run: bool) -> tuple[bool, str | None]:
    """Update __version__ = '...' in a Python __init__.py."""
    text = _read_text(path)
    m = _RE_INIT_VERSION.search(text)
    if not m:
        logger.warning("No __version__ found in %s", path)
        return False, None

    old = text[m.start(1) + len(m.group(1)):m.end(2) - len(m.group(2))]
    if old == version:
        return False, old

    new_text = _RE_INIT_VERSION.sub(rf"\g<1>{version}\2", text, count=1)
    if not dry_run:
        _write_text(path, new_text)
    return True, old


def update_json_version(path: Path, version: str, dry_run: bool) -> tuple[bool, str | None]:
    """Update "version" in a JSON file (package.json)."""
    text = _read_text(path)
    data = json.loads(text)
    old = data.get("version")
    if old == version:
        return False, old

    data["version"] = version

    if not dry_run:
        # Preserve 2-space indent and trailing newline (npm/node convention)
        _write_text(path, json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    return True, old


def update_go_version(path: Path, version: str, dry_run: bool) -> tuple[bool, str | None]:
    """Update const Version = "..." in a Go version constant file."""
    text = _read_text(path)
    m = _RE_GO_VERSION.search(text)
    if not m:
        logger.warning("No 'const Version' found in %s", path)
        return False, None

    old = text[m.start(1) + len(m.group(1)):m.end(2) - len(m.group(2))]
    if old == version:
        return False, old

    new_text = _RE_GO_VERSION.sub(rf"\g<1>{version}\2", text, count=1)
    if not dry_run:
        _write_text(path, new_text)
    return True, old


def update_gomod_require(
    path: Path,
    version: str,
    target_module: str,
    dry_run: bool,
) -> tuple[bool, str | None]:
    """
    Update the version on a `require <target_module> v...` line in a go.mod
    file.  Leaves `replace` directives untouched (the regex only matches the
    require form).
    """
    text = _read_text(path)
    pattern = _make_gomod_re(target_module)
    m = pattern.search(text)
    if not m:
        logger.warning(
            "No require line for %s found in %s", target_module, path
        )
        return False, None

    old = m.group(2)
    if old == version:
        return False, old

    new_text = pattern.sub(rf"\g<1>{version}", text, count=1)
    if not dry_run:
        _write_text(path, new_text)
    return True, old


STRATEGY_MAP = {
    "pyproject": update_pyproject,
    "init_py": update_init_py,
    "package": update_json_version,
    "go_version": update_go_version,
    "gomod_require": update_gomod_require,
}

# Strategies that take an extra positional argument (the 3rd tuple element)
# before the final `dry_run` flag.  Add new entries here when introducing
# strategies that need a parameter beyond (path, version).
_STRATEGIES_WITH_EXTRA_ARG = {"gomod_require"}

# ---------------------------------------------------------------------------
# Semver validation
# ---------------------------------------------------------------------------

_RE_SEMVER = re.compile(
    r"^\d+\.\d+\.\d+"  # major.minor.patch
    r"(?:-[0-9A-Za-z.-]+)?"  # optional pre-release
    r"(?:\+[0-9A-Za-z.-]+)?$"  # optional build metadata
)


def validate_version(version: str) -> bool:
    return bool(_RE_SEMVER.match(version))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def load_versions() -> dict[str, str]:
    """Load and validate versions.yaml."""
    if not VERSIONS_YAML.exists():
        sys.exit(f"versions.yaml not found at {VERSIONS_YAML}")

    with open(VERSIONS_YAML, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        sys.exit(f"versions.yaml must be a YAML mapping, got {type(data).__name__}")

    versions: dict[str, str] = {}
    for key, val in data.items():
        ver = str(val)
        if not validate_version(ver):
            sys.exit(f"Invalid semver for '{key}': {ver}")
        versions[key] = ver

    return versions


def run(*, check: bool = False, verbose: bool = False) -> int:
    """
    Synchronize versions.  Returns 0 on success, 1 if --check finds drift.
    """
    versions = load_versions()
    changes: list[str] = []
    errors: list[str] = []

    for project, target_version in sorted(versions.items()):
        rules = PROJECT_RULES.get(project)
        if rules is None:
            errors.append(
                f"'{project}' is in versions.yaml but has no rules in "
                f"update-versions.py — add an entry to PROJECT_RULES"
            )
            continue

        if not rules:
            # Empty rules list — version tracked externally (e.g. git tags)
            if verbose:
                logger.debug("  %s: no files to update (version via git tags)", project)
            continue

        for rule in rules:
            strategy, rel_path = rule[0], rule[1]
            abs_path = OSS_ROOT / rel_path
            if not abs_path.exists():
                errors.append(f"File not found: {abs_path}")
                continue

            updater = STRATEGY_MAP[strategy]
            if strategy in _STRATEGIES_WITH_EXTRA_ARG:
                if len(rule) < 3:
                    errors.append(
                        f"Strategy '{strategy}' requires a 3rd tuple element "
                        f"(rule: {rule!r})"
                    )
                    continue
                extra = rule[2]
                changed, old_version = updater(
                    abs_path, target_version, extra, check
                )
                label = f"{rel_path} [{extra}]"
            else:
                changed, old_version = updater(abs_path, target_version, check)
                label = str(rel_path)

            if changed:
                msg = f"  {label}: {old_version} -> {target_version}"
                changes.append(msg)
                if verbose or check:
                    logger.info(msg)
            elif verbose:
                logger.info("  %s: already %s", label, target_version)

    # Report
    if errors:
        logger.error("Errors encountered:")
        for e in errors:
            logger.error("  %s", e)

    if check:
        if changes:
            logger.error(
                "Version drift detected (%d file(s) out of sync):", len(changes)
            )
            for c in changes:
                logger.error(c)
            return 1
        else:
            logger.info("All versions in sync.")
            return 0

    if changes:
        logger.info("Updated %d file(s):", len(changes))
        for c in changes:
            logger.info(c)
    else:
        logger.info("All versions already in sync — nothing to update.")

    if errors:
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronize subproject versions from versions.yaml",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Dry-run: report drift and exit 1 if any file is out of sync",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show every file inspected, not just changes",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(message)s",
    )

    sys.exit(run(check=args.check, verbose=args.verbose))


if __name__ == "__main__":
    main()
