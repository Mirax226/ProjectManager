# Recovery Checkpoint

## Resume Here

The current ProjectManager phase is PJ-010 DailySystem adapter and Windows Excel evidence. PJ-001 through PJ-010 implementation foundations are present, and the latest suite is green: 150 tests passed.

Start by reading, in order:

1. `docs/project-memory/CURRENT-STATE.md`
2. `docs/project-memory/ARCHITECTURE-DECISIONS.md`
3. `docs/project-memory/ROADMAP.md`
4. the relevant PJ handoff under `docs/handoffs/`
5. source tests before changing a contract

## Pending Implementation

- Durable provider registry and explicit provider token revocation.
- Non-production provider and event fixtures with lifecycle transitions.
- Final operational event producer audit, including retry/backoff evidence.
- Durable storage decision for desktop profiles and Excel assets.
- Cloud Runner connectivity and production Windows verification.
- Telegram production connectivity and Cloud Runner verification.
- Archive retention, restore, and migration rehearsal policy.
- Independent PJ-010 closure review, including the recorded local Excel openability failure and any required workstation remediation.

## Recovery Rules

- Preserve project isolation on every read, write, claim, heartbeat, and provider lookup.
- Keep typed jobs validation-only until their adapter, permissions, approvals, bounded outputs, timeout, idempotency, and recovery behavior are tested.
- Do not enable production integrations, execute production jobs, expose secrets, modify tokens, deploy, commit, or push as part of recovery work.
- Treat the absent `.git` metadata as an unresolved identity gap; do not initialize a repository or substitute another checkout for this baseline.

## Safety State

This checkpoint records no production deployment, production mutation, secret change, destructive action, commit, or push. It changes documentation only.

## Next Action

Proceed to independent PJ-010 closure review. Keep authoritative Excel publication, Telegram transport, production DailySystem connectivity, and destructive retention actions disabled until separately approved and verified.
