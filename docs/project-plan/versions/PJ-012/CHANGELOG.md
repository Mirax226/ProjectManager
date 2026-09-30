# PJ-012 Cloudflare migration

- Started from clean 7a2a6f010d3f4756aaf76816c0a1faa303839e99 == origin/main; PJ-011B normal push/clean finalization recorded.
- Owner replaced runtime discovery with Cloudflare-first/D1 architecture; Render and old Config DB unused, no old-data migration or deletion.
- Retained two interrupted Excel diagnostic edits: stage timing and session/HWND/PID/start-time evidence; no new host matrix or source workbook writes.
- Unified Telegram webhook and existing Control Plane in one module Worker. Shared Ops Center models/typed actions reused; legacy polling opt-in only and legacy warmup separately opt-in.
- Added bounded update/body validation, administrator/private-chat authorization, D1 replay leases and idempotent diagnostic jobs, safe Telegram webhook setup/verification tooling.
- Separated D1 migration directory from historical Postgres migrations; added update-delivery metadata and result-runner provenance.
- Corrected cross-isolate D1 claim/result/idempotency handling and updated Runner attempt metadata; project scope and local execution boundaries retained.
- Added actual workerd/Miniflare/D1 integration and local Runner health E2E tests. CLOUDFLARE_DEPLOYMENT_PENDING_SECRETS: account confirmed by owner; D1 created and all three remote migrations applied. Worker deployment, webhook activation and live smoke checks await administrator IDs and interactive secret provisioning.
- Finalizer remains DryRun only; owner publishes external Plans/Review.

Owner follow-up: Telegram bootstrap administrator 843686302 configured in wrangler.jsonc. Remaining activation gate is interactive secret provisioning and live CLI verification/deployment.
