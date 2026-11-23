# PolySnipe

[![PolySnipe CI](https://github.com/Quackw/PolySnipe/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Quackw/PolySnipe/actions/workflows/ci.yml)

PolySnipe monitors Polymarket wallets and sends activity alerts to Telegram. V1 polls the Polymarket Data API. V3 FAST processes trades from Polymarket's real-time data stream (RTDS) and can use a second receiver.

## Components

| Component | Role |
| --- | --- |
| [V1 polling bot](src/) | Manages chat subscriptions and queues alerts in SQLite as it advances each wallet's checkpoint. |
| [V3 FAST](components/holy-snipe-fast/) | Processes RTDS trades and sends alerts through a shared queue with per-recipient deduplication. |
| [B receiver](components/snipe-b-receiver/) | Adds a second RTDS input and captures eligible events during a FAST bridge outage for reconciliation. |
| [V2 menu runtime](components/snipe-v2-runtime/) | Provides the Telegram menu and subscription handling used by FAST. |

## Test locally

Use Node.js 22.13 or newer, pnpm 10.32.1, Python 3 and OpenSSL. The test suite uses synthetic events, temporary databases and local sockets.

```sh
git clone https://github.com/Quackw/PolySnipe.git
cd PolySnipe
pnpm install --frozen-lockfile
pnpm run test:all
```

## Run V1

1. Copy [`.env.example`](.env.example) to `.env` and set your own Telegram bot token and a private SQLite path.
2. Run `pnpm run build` and `pnpm start`.
3. Open the bot in Telegram and use **Subscribe** to add a wallet. Alerts start after the subscription's initial checkpoint.

See the [operating guide](docs/operation.md) for configuration and the requirements for running FAST and B.

## Delivery behavior

- V1 stores a wallet's new checkpoint and queued alerts together in SQLite.
- FAST reserves each action per recipient before sending, so matching A and B events share one delivery record. The final delivery-state check has a two-second freshness limit.
- Ambiguous Telegram sends remain `UNKNOWN` in V1 or `DELIVERY_UNKNOWN` in FAST for review. Both senders keep those alerts out of automatic retries.
- B checks the current subscription scope before resuming live forwarding after reconnect. Events captured during the outage remain available for reconciliation.

## Documentation

- [Architecture](docs/architecture.md) traces both inputs, the shared delivery queue and the Telegram senders.
- [Operation](docs/operation.md) covers setup, runtime state and the authenticated B bridge.
- [Contributing](CONTRIBUTING.md) lists the local checks and pull request process.
