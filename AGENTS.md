# PolySnipe contributor guidance

- Keep V1 chat ownership, admin checks, subscription checkpoints, and durable delivery state intact.
- Preserve the shared FAST send-once identity and the 2000 ms delivery-state freshness limit. Keep unknown send outcomes fenced.
- Include synthetic fixtures only. Keep `.env`, subscriber scopes, keys, state databases and delivery logs outside Git.
- Run `pnpm run test:all` before publishing behavior changes. Use fixture transactions and local sockets for delivery and ingress tests.
- Keep [the project map](docs/architecture.md) and [configuration notes](docs/operation.md) current.
