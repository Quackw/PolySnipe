# PolySnipe

PolySnipe watches Polymarket wallets and sends activity alerts to Telegram. It combines a polling bot with a faster stream-based path for trade alerts.

| Component | What it does |
| --- | --- |
| [V1 polling bot](src) | Manages chat subscriptions, reads activity from Data API v2, and writes alerts to a durable SQLite queue before sending. |
| [V3 / FAST](components/holy-snipe-fast) | Processes RTDS trade events through one delivery queue. Checks delivery state, prevents duplicate sends, and records ambiguous Telegram outcomes. |
| [B receiver](components/snipe-b-receiver) | Provides a second RTDS input. The recovery candidate keeps receiving and saving events when its channel to FAST goes down. |
| [V2 menu runtime](components/snipe-v2-runtime) | JavaScript compatibility code used by the FAST Telegram menu. |

## Try it locally

Use Node.js 22.13 or newer, pnpm 10, Python 3 and OpenSSL. The tests use dummy trade events and temporary databases. No Telegram token, real subscriptions or market connection is needed.

```sh
pnpm install --frozen-lockfile
pnpm run test:all
```

For a V1 bot of your own, copy `.env.example` to `.env`, set a Telegram bot token and a private SQLite path, then run `pnpm run build` and `pnpm start`. Open your bot in Telegram and use **Subscribe** to add a wallet. A new subscription starts with a checkpoint and only alerts on later activity. See [configuration and operating notes](docs/operation.md).

## Delivery model

- V1 commits the activity checkpoint and its new alerts together. Each subscription belongs to a Telegram chat.
- FAST reserves a canonical action for each recipient before a send. A and B use the same SQLite queue and apply a two-second limit to delivery-state freshness.
- A send that may have reached Telegram without a valid acknowledgment remains `UNKNOWN` for review.
- B buffers events while the FAST channel is down. After authentication and a current scope check, it resumes sending new events to FAST. Saved outage events are available for reconciliation.

See [the project map](docs/architecture.md) for source locations and coverage. GitHub Actions runs tests and a build.
