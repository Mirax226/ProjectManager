# ProjectManager hybrid architecture

ProjectManager now has two explicit runtime boundaries:

* The legacy Node runtime (`src/bot.js` and the root stores) keeps PostgreSQL, Telegram admin UX, GitHub, deployments, logs, Safe Mode, Ops Timeline, and the existing web dashboard.
* The control plane (`src/controlPlane`) is Fetch-compatible and can run as a Cloudflare Worker. It owns project-scoped operational events, runner heartbeat state, typed jobs, leases, alerts, and status reads. A D1 binding (`CONTROL_PLANE_DB`) persists only this control-plane state. `src/controlPlane/server.js` provides a local Node adapter for development and tests.
* The Windows runner (`src/runner`) is the only new runtime allowed to invoke Git, npm, PowerShell/Excel, filesystem checks, or the local Codex CLI. It uses argument arrays with `shell:false`; Telegram and the Worker never expose arbitrary shell execution.

The existing PostgreSQL/config DB remains authoritative for existing PG features. The D1 migration is intentionally a small control-plane schema and is not a duplicate of the PG database.

The Worker has an explicit in-memory fallback for local validation when no D1 binding is supplied. Production configuration should always bind D1 before deployment.

PJ-ZJ-001 adds a static project registry in `src/controlPlane/projectRegistry.js`. `daily-system` retains its existing typed job/capability set; `zj` has a separate safe set. Registry IDs, not user-provided paths, select allowed operations. The existing PJ D1 tables carry both projects; the PJ-side `PJ_ZJ_ENABLED` flag does not change ZJ's own optional `PJ_ENABLED` mode. Local ZJ reads and validation stay on a project-scoped Windows Runner; the Worker handles D1, authentication, Telegram and readiness aggregation. Production ZJ resources are never mutation targets in this milestone.
