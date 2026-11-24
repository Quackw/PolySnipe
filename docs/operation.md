# Running PolySnipe

## V1

1. Install dependencies with `pnpm install --frozen-lockfile`.
2. Copy `.env.example` to `.env`. Set `TELEGRAM_BOT_TOKEN` for your bot and a private `SQLITE_PATH`. Set `ADMIN_TELEGRAM_USER_IDS` only for accounts that should see the admin menu.
3. Run `pnpm run build` and `pnpm start`. Use a separate bot token and database for a development instance.

V1 stores subscriptions, checkpoints and alert delivery state in SQLite. Preserve the database across normal restarts. Avoid running two pollers against the same bot and state. Polling and request limits can be set in `.env`. Blank numeric settings use their defaults. A configured `HTTP_RETRIES` is the fallback for `POLYMARKET_RETRIES`. With neither set, V1 uses two general HTTP retries and one Polymarket retry.

## FAST and B

Use `pnpm run test:components` to exercise the receive workers, shared sender and bridge with temporary data. It makes no requests to Telegram or Polymarket.

A live setup needs a private FAST state directory and subscription database, a pinned bot identity, an authenticated B bridge, and an approved subscription scope. FAST requires `POLYSNIPE_FAST_ENABLE=1`, an absolute `POLYSNIPE_FAST_STATE_DIR` owned by its process user with mode `0700`, `TELEGRAM_BOT_TOKEN`, `EXPECTED_TELEGRAM_USERNAME` and the owner configuration. It verifies the bot identity before opening a market stream. The `ENABLE_CANARY` and `FAST_DISABLED` markers are operator-owned state. FAST REDEEM and recovery default to off.

B requires `POLYSNIPE_B_ENABLE=1` and an absolute `POLYSNIPE_B_STATE_DIR` owned by its process user with mode `0700`. Its config, frozen scope, pinned hosts and key material are private files in that directory. The forced SSH command relays only to the fixed private FAST socket. Use a restricted account for the bridge.

Treat `UNKNOWN` as unresolved delivery. The B store retains outage events for reconciliation. A full store reports incomplete capture, so check its health and free space during a trial.

## Systemd

Install dependencies with `pnpm install --frozen-lockfile` at the chosen Git revision. Run `pnpm run build` on the V1 host. Use the [templates](../deploy/systemd/) for the services that belong on each host. B can run on its own host and uses its private SSH bridge to FAST.

1. Copy the selected templates. Replace `POLYSNIPE_USER` and `/path/to/PolySnipe`. Set the absolute Node.js 22.13+ path in `ExecStart` if Node is installed elsewhere.
2. Supply each unit's `EnvironmentFile` with the corresponding variables described above. Keep these files private. Provision each service's state directory and preserve its database and bridge state across restarts.
3. Verify the rendered units with `systemd-analyze verify`. Install them into `/etc/systemd/system/`, run `systemctl daemon-reload` and then enable and start the selected units. Check service status and logs with `systemctl status` and `journalctl -u`. The units give each service 25 seconds to shut down.

## Delivery and shutdown

V1 and FAST give SQLite writers up to five seconds to acquire a lock. FAST still limits the age of its final delivery-state verification to two seconds before starting a request.

If FAST stops or rejects a claim before starting the request, it releases the owned claim back to the queue. Inactive subscriptions are cancelled. A bad recovery acknowledgement is stored as `DELIVERY_UNKNOWN` in the delivery queue. V1 likewise keeps a partially acknowledged multi-chunk alert in `UNKNOWN` if a later chunk fails. These states stay out of automatic retries.

V1 cancels pending outbox rows in the same transaction that removes their subscription. Existing send attempts can finish, and their results stay in the outbox history. Dispatch preserves FIFO within each chat during backoff. A retryable rejection on the tenth attempt puts the row in `REJECTED` and lets the next queued alert proceed.
