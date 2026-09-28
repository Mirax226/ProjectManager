# Architecture Decisions

## Ownership Boundaries

- ProjectManager owns operational coordination, authorization, event storage, alerting, job lifecycle, runner state, and sanitized operator views.
- External projects, including DailySystem, own business data, workbook semantics, sync/reconciliation rules, backup policy, and domain workflows.
- Telegram is an admin/operator interface, not a general execution console.

## Runtime And Data

- The legacy Node runtime remains responsible for existing bot, PostgreSQL/config DB, GitHub, deployment, logs, and dashboard behavior.
- The Control Plane is Fetch-compatible and uses an isolated D1 schema for control-plane state; it is not a duplicate PostgreSQL business database.
- The Windows Runner is the local execution boundary. Cloud Runner remains a future authenticated control-plane role and is not production-verified.

## Jobs And Execution

- Jobs are typed, project-bound, lease-controlled, timeout-bounded, idempotent where side effects are possible, and return sanitized results.
- Excel/provider jobs introduced in PJ-005 through PJ-007 are validation-only and are excluded from the live executable allowlist.
- Runner commands use reviewed argument arrays with `shell: false`; arbitrary command text, unrestricted PowerShell, secret access, destructive database actions, and unrestricted deployment are forbidden.

## Integration Boundary

The intended boundary is:

```text
External project / DailySystem adapter
              |
       authenticated HTTPS API
              |
ProjectManager Control Plane
              |
     project-bound Windows Runner
```

Project credentials must be scoped, revocable, and compatible with a future HMAC scheme. The design does not depend on a Cloudflare Service Binding.

## Security And Observability

- Event context, provider metadata, logs, diagnostics, and Telegram messages are bounded and sanitized.
- Tokens, API keys, passwords, DSNs, cookies, authorization values, private keys, and raw workbook contents must not reach operator messages or event context.
- Project scope is enforced at authenticated boundaries rather than trusted from request payloads.

## Archive Strategy

- Keep PJ handoffs immutable as historical audit records; add a new handoff for each phase instead of rewriting prior evidence.
- Keep `docs/project-memory/` as the current recovery index. Update it when architecture, contracts, or verified baselines change.
- Archive operational events according to a future retention policy with bounded payloads, correlation IDs, project identity, and recovery evidence. Do not archive secrets or business workbook contents.
- Any migration or archival change requires a non-production rehearsal, restore verification, and explicit ownership decision before production use.

## Shared Archive And Managed Configuration

- There is exactly one shared private Telegram archive channel across projects. Archive records always retain `projectId` and are independently addressable and restore-scoped.
- Operational configuration is typed, validated, attributable, timestamped, sanitized, and idempotent when an operation key is supplied.
- Telegram archive transport is abstracted and disabled by default. In-memory/local test providers are permitted; production sends require a separate approval and adapter review.
- Archive/backup retention APIs provide eligibility metadata only. No automatic deletion is authorized by age or candidate status.

## PJ-009 Durable Operations

- New operational entities use the existing Control Plane D1 database through additive, rerun-safe `cp_*` tables; there is no competing persistence subsystem.
- In-memory and D1 stores expose the same operational-state methods. D1 hydration ignores malformed operational JSON rather than promoting it to trusted state.
- Excel snapshot/backup execution is filesystem-copy and hash-verification based. The source workbook is never overwritten, VBA is never executed, and authoritative sync/publication remains disabled.
- Runner result events are emitted only from a typed allowlist and derive project identity from the leased job. Recovery categories update the existing incident/alert state rather than introducing another alert engine.

## PJ-010 Non-Production DailySystem And Excel Evidence

- DailySystem integration is a typed, bounded HTTPS adapter. It propagates a caller correlation ID, classifies timeout/unavailable/unauthorized/malformed failures, limits response bodies, and persists only allowlisted operational fields. It does not import DailySystem domain logic or persist business payloads.
- Excel Desktop openability checks use a temporary disposable workbook copy. The fixed COM probe disables macros/VBA, events, link updates, and prompts, opens read-only, closes the owned COM instance, and reports owned process IDs when observable. Source SHA-256 is captured before and after every probe; authoritative source files are never saved or overwritten.
- DailySystem/Excel failure and recovery evidence is routed through the existing Control Plane event and alert lifecycle. Reconciliation identities are project-scoped, duplicate evidence is idempotent, and only safe status/count/hash/diagnostic metadata is retained. `EXCEL_SYNC` remains disabled.
