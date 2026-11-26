# PolySnipe

[![PolySnipe CI](https://github.com/Quackw/PolySnipe/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Quackw/PolySnipe/actions/workflows/ci.yml)

PolySnipe monitors Polymarket wallets and sends activity alerts to Telegram. V1 polls the Polymarket Data API. V3 FAST processes trades from Polymarket's real-time data stream (RTDS) and can use a second receiver.

## Components

| Component | Role |
| --- | --- |
| [V1 polling bot](src/) | Manages chat subscriptions and queues alerts in SQLite as it advances each wallet's checkpoint. |
| [V3 FAST](components/holy-snipe-fast/) | Processes RTDS trades and sends alerts through a shared queue with per-recipient deduplication. |
| [B receiver](components/snipe-b-receiver/) | Adds a second RTDS input and captures eligible events during a FAST bridge outage for reconciliation. |
| [V2 menu runtime](components/snipe-v2-runtime/) | Compatibility code that provides the Telegram menu and subscription handling used by FAST. |
| [RTDS receivers and gates](components/snipe-rtds-resilient/) | FAST runtime support for buffered input, reconnects and fresh delivery checks. |
| [FAST bridge](components/snipe-v3-combined/) | FAST runtime support for the authenticated B channel and shared queue. |
| [Readiness](components/snipe-v3-readiness/) and [passive diagnostics](components/snipe-rtds-passive-ab/) | Eligibility and bridge helpers, plus tools for validating the RTDS inputs. |

## Test locally

Use Node.js 22.13 or newer, pnpm 10.32.1, Python 3 and OpenSSL. The test suite uses synthetic events, temporary databases and local sockets.

```sh
git clone https://github.com/Quackw/PolySnipe.git
cd PolySnipe
pnpm install --frozen-lockfile
pnpm run lint
pnpm run test:all
```

## Run V1

1. Copy [`.env.example`](.env.example) to `.env` and set your own Telegram bot token and a private SQLite path.
2. Run `pnpm run build` and `pnpm start`.
3. Open the bot in Telegram and use **Subscribe** to add a wallet. Alerts start after the subscription's initial checkpoint.

See the [operating guide](docs/operation.md) for configuration and the requirements for running FAST and B.

## Run under systemd

The [unit templates](deploy/systemd/) cover V1, FAST and the independent B receiver. Install the pinned dependencies, build V1, and fill in the service user, checkout path and private environment file for each selected unit. Follow the [systemd setup](docs/operation.md#systemd) to verify the units and install them on the intended hosts.

The [B runtime subtree](components/snipe-b-receiver/ops/) contains the modules needed to run B on its own host. Its RTDS connection, managed socket and native worker are synchronized with the shared RTDS modules so that the subtree can be deployed on its own.

## Delivery behavior

- V1 stores the checkpoint and queued alerts together. Unsubscribing cancels pending alerts in the same operation. Alerts keep their order within each chat, with at most ten delivery attempts per row.
- FAST reserves each action per recipient before sending, so matching A and B events share one delivery record. The final delivery-state check has a two-second freshness limit.
- Ambiguous Telegram sends remain `UNKNOWN` in V1 or `DELIVERY_UNKNOWN` in FAST for review. Both senders keep those alerts out of automatic retries.
- B checks the current subscription scope before resuming live forwarding after reconnect. Events captured during the outage remain available for reconciliation.

## Documentation

- [Architecture](docs/architecture.md) traces both inputs, the shared delivery queue and the Telegram senders.
- [Operation](docs/operation.md) covers setup, runtime state and the authenticated B bridge.
- [Contributing](CONTRIBUTING.md) lists the local checks and pull request process.
