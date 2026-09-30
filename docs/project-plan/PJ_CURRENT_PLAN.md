# ProjectManager Current Plan

- Canonical repository: C:\Users\Amir\Documents\GitHub\cloned\ProjectManager.
- PJ-011B finalized at 7a2a6f010d3f4756aaf76816c0a1faa303839e99; normal push succeeded, working tree clean, HEAD == origin/main verified.
- PJ-012 owner decision: Cloudflare = designated production runtime; D1 = active PJ operational persistence; Windows Runner = local execution plane. Render and old Postgres = DEPRECATED / UNUSED / NON-AUTHORITATIVE; NO MIGRATION REQUIRED. External resources are not deleted.
- Runtime target: Telegram webhook -> unified projectmanager-control-plane Worker -> shared application/Ops Center -> D1/jobs -> project-bound authenticated Windows Runner. Production Worker imports no Node bot bootstrap/Config DB/Excel/shell execution.
- Implementation: bounded secret-verified webhook, private-chat admin authorization, durable replay leases, shared admin/status/incident/runner UI, safe typed diagnostic jobs; existing Control Plane routes preserved/aliased. Legacy Node polling and Config DB warmup explicitly opt-in.
- Persistence: D1-only migrations under migrations/d1; cross-isolate atomic claims, result compare-and-swap and attempt/terminal-owner verification. Managed config and audit reuse existing D1 store.
- Deployment status: CLOUDFLARE_DEPLOYMENT_PENDING_SECRETS: account confirmed by owner; D1 created and all three remote migrations applied. Worker deployment, webhook activation and live smoke checks await administrator IDs and interactive secret provisioning.
- Excel: PJ-011B intermittent COM/open/read/quit evidence retained, underlying cause unresolved. PJ-012 keeps diagnostics and adds stage/provenance metadata; no authoritative XLSM/VBA changes, no new host probes, no sync.
- Validation and exact test totals: docs/handoffs/PJ-012/REVIEW.md. Activation commands/safety and parity limits: CLOUDFLARE-RUNBOOK.md.
- Finalizer: Codex DryRun only; owner publishes append-only external Review/Plans manually.
- Next milestone: authenticated Cloudflare activation and safe Telegram->D1->Windows Runner production health smoke; Excel host investigation separately observable. No additional architecture/paid-service decision needed.
