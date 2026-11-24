# Contributing to PolySnipe

1. Start a focused branch from `main`.
2. Use the [README setup](README.md#test-locally) and run `pnpm run test:all` before opening a pull request.
3. Describe the behavior that changed, link related issues when available and include the checks you ran.
4. Review the GitHub CI result before merging. GitHub Actions runs type checking, tests and the build on pull requests and pushes to `main`.

Use synthetic fixtures for runtime tests and bug reports. Keep real Telegram tokens, wallet lists, subscriber identifiers, databases and delivery logs outside Git. Update the [architecture](docs/architecture.md) and [operating guide](docs/operation.md) when behavior changes.

Changes to the delivery path must preserve subscription ownership, the shared SEND-ONCE identity, the two-second delivery-state freshness limit and the fencing of ambiguous sends.
