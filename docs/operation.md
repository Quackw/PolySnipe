# Running PolySnipe

## V1

1. Install dependencies with `pnpm install --frozen-lockfile`.
2. Copy `.env.example` to `.env`. Set `TELEGRAM_BOT_TOKEN` for your bot and a private `SQLITE_PATH`. Set `ADMIN_TELEGRAM_USER_IDS` only for accounts that should see the admin menu.
3. Run `pnpm run build` and `pnpm start`. Use a separate bot token and database for a development instance.

V1 stores subscriptions, checkpoints and alert delivery state in SQLite. Preserve the database across normal restarts. Avoid running two pollers against the same bot and state. Polling and request limits can be set in `.env`.

## FAST and B

Use `pnpm run test:components` to exercise the receive workers, shared sender and bridge with temporary data. It makes no requests to Telegram or Polymarket.

A live setup needs a private FAST state directory and subscription database, a pinned bot identity, an authenticated B bridge, and an approved subscription scope. FAST requires `POLYSNIPE_FAST_ENABLE=1`, an absolute `POLYSNIPE_FAST_STATE_DIR` owned by its process user with mode `0700`, `TELEGRAM_BOT_TOKEN`, `EXPECTED_TELEGRAM_USERNAME` and the owner configuration. It verifies the bot identity before opening a market stream. The `ENABLE_CANARY` and `FAST_DISABLED` markers are operator-owned state. FAST REDEEM and recovery default to off.

B requires `POLYSNIPE_B_ENABLE=1` and an absolute `POLYSNIPE_B_STATE_DIR` owned by its process user with mode `0700`. Its config, frozen scope, pinned hosts and key material are private files in that directory. The forced SSH command relays only to the fixed private FAST socket. Use a restricted account for the bridge.

Treat `UNKNOWN` as unresolved delivery. The B store retains outage events for reconciliation. A full store reports incomplete capture, so check its health and free space during a trial.
