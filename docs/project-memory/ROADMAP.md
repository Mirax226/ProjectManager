# Roadmap

## Completed Foundations

- PJ-001: audit checkpoint and architecture baseline.
- PJ-002: Control Plane contract and multi-project boundary hardening.
- PJ-003: Telegram operations center views and safe actions.
- PJ-004: runner and integration boundary audit.
- PJ-005: typed Excel job design.
- PJ-006: desktop profiles and registered Excel asset validation.
- PJ-007: provider metadata, operational event categories, DailySystem status model, and validation-only jobs.

## Next Milestones

### PJ-008 - Repository Provenance, Configuration, And Archive Foundation (completed)

Managed configuration, shared archive contracts, backup metadata, retention candidates, safe admin summaries, and provenance investigation are complete. Production Telegram transport and physical Excel execution remain disabled.

### PJ-009 - Durable Operations And Runner/Excel Integration (completed)

Additive D1 operational persistence, typed runner Excel health/snapshot/backup/reconciliation operations, provider health fixtures, allowlisted job-result events, and safe Telegram typed-job surfaces are complete. Production Excel publication, Telegram transport, and deletion remain disabled.

### PJ-010 - DailySystem Adapter Verification And Controlled Non-Production Execution (implemented; independent closure review pending)

Typed DailySystem health/status/Excel/reconciliation adapter, bounded failure classification, fixture coverage, controlled disposable-copy Excel evidence, source fingerprint verification, event/incident lifecycle integration, and safe Telegram operational views are implemented. Production connectivity, authoritative publication, and real Telegram transport remain disabled.

### PJ-010 - Persistence and Recovery Policy

Decide durable ownership for provider metadata, desktop profiles, and Excel assets. Define retention, archive, restore, migration, and recovery evidence before any production enablement.

### Later Enablement Gate

Only after explicit review: enable a narrowly scoped non-production runner adapter, then separately review production connectivity, approval policy, secrets handling, rollback, and monitoring. No milestone authorizes arbitrary execution or unrestricted deployment.

## Ordering Constraints

1. Close contract and project-scope evidence.
2. Add fixture and failure-path coverage.
3. Define persistence and archive/recovery policy.
4. Review adapter permissions and operational approvals.
5. Consider integrations only after non-production evidence is complete.
