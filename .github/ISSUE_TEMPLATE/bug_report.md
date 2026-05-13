---
name: Bug report
about: Report a reproducible problem with the Aether gateway or one of its SDKs
title: "[BUG] "
labels: bug
assignees: ''
---

## Bug Description

A clear and concise description of what the bug is.

## Aether Version

<!-- Gateway version (shown in startup logs or `./gateway --version`) -->
Gateway version: `0.x.x`

## SDK Details

<!-- Which SDK are you using, and what version? -->
- SDK: Go / Python / TypeScript / (other)
- SDK version: `0.x.x`

## Gateway Configuration

<!-- Paste the relevant sections of your gateway config (YAML or env vars). REDACT all credentials, API keys, TLS private keys, and passwords. -->

```yaml
# relevant config here (redacted)
```

## Reproduction Steps

Steps to reproduce the behavior:

1. Start gateway with config above
2. Run the following client code / command: `...`
3. Observe: `...`

## Expected Behavior

What you expected to happen.

## Actual Behavior

What actually happened, including any error messages returned by the SDK or gateway.

## Logs

<!-- Paste relevant gateway logs (set log level to DEBUG if possible). Redact any sensitive values. -->

```
paste logs here
```

## Additional Context

- Deployment environment: Kubernetes / Docker Compose / bare metal / local dev
- Number of gateway instances: 
- Any recent changes to infrastructure or config: 
