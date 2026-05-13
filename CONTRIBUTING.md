# Contributing to Aether

Thank you for your interest in contributing to Aether. This document covers the contribution workflow, code style expectations, test requirements, and review process.

## Table of Contents

- [Getting Started](#getting-started)
- [Branching Workflow](#branching-workflow)
- [Code Style](#code-style)
- [Running Tests](#running-tests)
- [Filing Issues](#filing-issues)
- [Pull Request Process](#pull-request-process)
- [Development Dependencies](#development-dependencies)

---

## Getting Started

1. Fork the repository on GitHub.
2. Clone your fork locally:
   ```bash
   git clone https://github.com/<your-username>/aether.git
   cd aether
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/scitrera/aether.git
   ```

---

## Branching Workflow

- Work on a feature branch cut from `main`:
  ```bash
  git checkout main
  git pull upstream main
  git checkout -b feat/my-feature
  ```
- Use a descriptive prefix: `feat/`, `fix/`, `docs/`, `chore/`.
- Keep commits focused and atomic. Squash fixup commits before requesting review.
- Rebase onto `main` before opening a PR:
  ```bash
  git fetch upstream
  git rebase upstream/main
  ```

---

## Code Style

### Go (server, Go SDK)

- Format all Go code with `gofmt` before committing. The CI gate runs `gofmt -l` and fails on diffs.
- Follow standard Go idioms: exported identifiers have doc comments, errors are wrapped with context (`fmt.Errorf("context: %w", err)`), and no silent error swallowing.
- Run from the `server/` directory:
  ```bash
  gofmt -w ./...
  go vet ./...
  ```
- Linting (optional but appreciated):
  ```bash
  golangci-lint run
  ```

### Python (Python SDK)

- Format with `black` and sort imports with `isort`:
  ```bash
  cd sdk/python-client
  black .
  isort .
  ```
- Type annotations are expected on all public API surface. Run `mypy` to check:
  ```bash
  mypy scitrera_aether_client/
  ```

### TypeScript (TypeScript SDK)

- Format with `prettier`:
  ```bash
  cd sdk/typescript
  npx prettier --write src/
  ```
- Lint with ESLint:
  ```bash
  npx eslint src/
  ```

---

## Running Tests

### Server (Go)

Run from the `server/` directory. The short flag skips integration tests that require live infrastructure (RabbitMQ, Redis, PostgreSQL):

```bash
cd server
go test ./...            # all tests (requires running infra)
go test -short ./...     # unit tests only
```

Start local dev infrastructure with the provided scripts:

```bash
./scripts/docker_rmq_test.sh     # RabbitMQ with Streams plugin
./scripts/docker_valkey_test.sh  # Redis / Valkey cluster
```

### Python SDK

```bash
cd sdk/python-client
pip install -e ".[dev]"
pytest
```

### TypeScript SDK

```bash
cd sdk/typescript
npm install
npm test
```

### Protobuf

If you modify `.proto` files in `api/proto/`, regenerate the generated code before committing:

```bash
bash scripts/compile_protos.sh
```

Commit both the `.proto` changes and the regenerated files together.

---

## Filing Issues

Use the GitHub issue templates:

- **Bug report** — include Aether version, SDK + version, relevant gateway config (redact credentials), reproduction steps, and logs.
- **Feature request** — describe the motivation, the proposed API or behavior, and alternatives you considered.

Before filing, search existing issues to avoid duplicates. For security vulnerabilities, **do not open a public issue** — contact security@scitrera.com directly.

---

## Pull Request Process

1. Ensure all tests pass and code is formatted (see above).
2. Update documentation in `server/docs/` or the relevant SDK `README.md` if your change affects behavior or API.
3. Open a PR against `main` with a clear title and description explaining *why* the change is needed, not just *what* changed.
4. A maintainer will review within a reasonable timeframe. Address review comments with new commits (do not force-push during an active review).
5. PRs require at least one maintainer approval before merge. Maintainers may squash-merge feature branches.
6. CI must pass: build, unit tests, `gofmt`/`prettier` format check, and proto consistency check.

### PR Title Convention

Use the same prefix convention as branch names: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`.

Example: `fix: handle ERR_CIRCUIT_OPEN retryable flag in Go SDK reconnect loop`

---

## Development Dependencies

| Tool | Version | Purpose |
|------|---------|---------|
| Go | 1.22+ | Server and Go SDK |
| Python | 3.10+ | Python SDK |
| Node.js | 22+ | TypeScript SDK |
| Docker | any recent | Local infra scripts |
| protoc + plugins | see `scripts/compile_protos.sh` | Proto codegen |

---

## Questions

If you have questions about architecture or want to discuss a larger change before implementing it, open a GitHub Discussion or file an issue tagged `question`.
