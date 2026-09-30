# PJ-011B Config DB and Runtime Diagnosis — 2026-09-30

## Runtime provenance

The active Node host is **ACTIVE_NODE_HOST_UNKNOWN**. Owner reports no local PM Node process, PM2, or configured Config DB variables. Neither variable exists in desktop process/User/Machine environment; canonical repository has no .env. The old tree has only .env.example and remains reference-only.

README.md explicitly documents Render build `npm install` and start `node src/bot.js`, plus a Render Postgres cutover setting DATABASE_URL_PM or PATH_APPLIER_CONFIG_DSN. Git history attributes the Render deployment notes to ff15588. This establishes intended hosting, not a live service name or revision. No tracked GitHub workflow, Dockerfile, Procfile, Railway/Fly/service manifest was found identifying another active bot deployment.

package.json `start` -> src/bot.js -> root bot.js.startBot -> startConfigDbWarmup. The bot deletes its own webhook and uses grammy long polling. Its handlers for registered project webhooks do not change this startup mode.

wrangler.jsonc configures projectmanager-control-plane -> src/controlPlane/worker.js. The Worker handles Control Plane APIs with CONTROL_PLANE_DB (D1), PG_CONTROL_PLANE_ADMIN_TOKEN, PG_RUNNER_TOKENS_JSON, and PG_PROJECT_TOKENS_JSON. It does not import the Node Telegram service, Postgres configDb, or warmup. DATABASE_URL_PM and PATH_APPLIER_CONFIG_DSN are not declared or read by that Worker. This does not prove whether extra secret names exist in a deployed Worker.

Wrangler is unavailable in PATH/node_modules; the Cloudflare dashboard redirects to login. No deployed secret names, values, configuration, or metadata were fetched or mutated. Repeated exact warmup messages match the Node path, not the checked-in Worker; identifying a particular active service still requires live provenance. Telegram observations are consistent with a legacy Node deployment or equivalent older code, not proof of a specific host.

## Source and parser trace

DATABASE_URL_PM (when nonempty) -> otherwise PATH_APPLIER_CONFIG_DSN -> tryFixPostgresDsn (credential encoding only) -> inspectConfigDbDsn -> pg Pool/pg-connection-string -> connect/DNS -> SELECT 1. normalizePostgresDsn in src/db/dsn.js belongs to separate DB Hub paths and is not called by Config DB warmup. DATABASE_URL is not a warmup DSN fallback; unrelated DATABASE_URL TLS hints were removed.

Actual deployed source variable: UNKNOWN. Actual parsed hostname: UNKNOWN. Presence of either variable in the deployed environment: UNVERIFIED.

The corrected owner-reported error is `(ENOTFOUND) tenant/user postgres.bqvyprmlqcbrwepnrwoc not found`. That identifier is the tenant-qualified user in this provider message, not evidence of a DNS hostname. Synthetic parser and real pg-query boundary tests show this user remains separate from the host before/after encoding repair. No production DSN was inspected, so the exact deployed field and parser output remain unverified.

Supabase's official troubleshooting article matches this message: the shared pooler could not match a host and username to a project. Copying the pooler host from the project's Connect dialog matters; its cluster index must not be guessed. This establishes the provider failure class, not which deployed field is incorrect:
https://supabase.com/docs/guides/troubleshooting/tenant-or-user-not-found

## Code changes

- Retained interrupted typed preflight/classification and source-precedence tests.
- Corrected host label/port/percent-encoding validation; IPv4/IPv6 structurally supported. Reject query host/port overrides so pg cannot dial an unvalidated alternate destination.
- Fixed repeated pool reads after first repair: every read uses the repaired in-memory DSN while ENV remains unchanged. Repair warning emits once and contains no DSN, password suffix, username, or query values.
- Typed errors: CONFIG_DB_DSN_MISSING, CONFIG_DB_DSN_INVALID, CONFIG_DB_HOST_INVALID, CONFIG_DB_DNS_FAILED, CONFIG_DB_AUTH_FAILED, CONFIG_DB_CONNECT_TIMEOUT, CONFIG_DB_TLS_FAILED, CONFIG_DB_QUERY_FAILED, UNKNOWN_DB_ERROR. Preserve typed categories through catches and legacy category compatibility.
- OS ENOTFOUND/getaddrinfo -> DNS; provider tenant/user not found (including XX000) -> AUTH/routing class. Unknown failures are not automatically invented as query failures.
- Structural, auth/routing, TLS, and known query failures halt retries for the boot. DNS/connect-timeout/unknown failures use 500ms exponential backoff, capped at 60s plus <=250ms jitter, default 20 attempts; cap constrained to 1..100. Missing configuration also halts.
- Existing ops incident store receives a stable project/source/category/host/root fingerprint; retry reasons, attempts, warning/exception wrappers and generated Ref IDs do not split a known DNS incident. Different source/class/root retains separate incidents. Acknowledgment does not reopen an outage; active Config DB incidents do not auto-resolve merely because retries stop.
- Telegram routing suppresses repeated occurrences of the same config-db incident, independent of the category debounce. Repeat count increases. Successful warmup resolves matching source incidents after successful config load; initial outages receive an outage ID and existing recovery notification contract allows one notice per outage.
- Full DSN masking removes suffix/query disclosure. CODEX_TASK output passes DB URI sanitation before generic redaction.

## Owner action

Configuration review required; no secret change is justified yet. Locate the active Node service (Render is the documented intended provider), inspect variable NAMES only, then execute tools/diagnose-config-db.js in that runtime to emit only safe parsed fields. It performs no network query or ENV mutation. Compare selected host and tenant-qualified username with the same project's Supabase Connect dialog. Expected components are Postgres scheme, percent-encoded credentials, copied provider hostname, supported port, and database name. Correct only the selected variable if a mismatch is confirmed, through a separately approved owner action. Never paste the DSN/password.

No production deployment/database mutation/configuration change occurred. Code bugs (classification, repair state, unvalidated override, alert routing) were repaired. The external provider routing failure is evidenced; its exact external configuration cause remains blocked by unavailable runtime metadata.
