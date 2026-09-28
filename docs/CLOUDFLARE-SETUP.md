# Cloudflare control plane setup

No Cloudflare resources are created by this repository. Before deployment, manually:

1. Create a D1 database and replace `REPLACE_MANUALLY` in `wrangler.jsonc` with its ID.
2. Apply `migrations/20260924_control_plane.sql` to that D1 database using the reviewed, environment-specific migration process.
3. Configure Worker secrets/vars for `PG_CONTROL_PLANE_ADMIN_TOKEN`, `PG_RUNNER_TOKENS_JSON`, and `PG_PROJECT_TOKENS_JSON`. Use JSON maps such as `{ "daily-system-windows": { "token": "<token>", "project": "daily-system" } }`; never commit them.
4. Set an explicit allowed origin if a browser dashboard will call the API (`PG_CONTROL_PLANE_CORS_ORIGIN`).
5. Run local validation (`npm.cmd run check`, focused tests, and Wrangler's local dev/config validation) before a separately approved deployment.

The Worker exposes `GET /healthz`, `GET /api/v1/status`, `GET /api/v1/projects`, `GET /api/v1/jobs`, `GET /api/v1/jobs/:id`, `POST /api/v1/ops/events`, `POST /api/v1/runners/heartbeat`, `POST /api/v1/jobs`, `POST /api/v1/jobs/claim`, and `POST /api/v1/jobs/:id/result`. All API routes require a scoped bearer token; `/healthz` is intentionally public.
