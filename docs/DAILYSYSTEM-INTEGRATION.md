# DailySystem integration

DailySystem is registered as project key `daily-system` (display name `DailySystem`). PG records operational facts only: application health/errors, D1 and Telegram events, runner/Excel diagnostics, test status, and Codex task results. It does not copy DailySystem business data or reconciliation rules.

Operational events use `POST /api/v1/ops/events` and this shape:

```json
{
  "schemaVersion": 1,
  "eventId": "stable-id",
  "project": "daily-system",
  "environment": "dev",
  "severity": "ERROR",
  "category": "EXCEL_RECONCILIATION_ERROR",
  "timestamp": "2026-09-24T10:00:00.000Z",
  "message": "safe summary",
  "context": { "correlationId": "..." },
  "source": "daily-system",
  "correlationId": "..."
}
```

Supported categories include APP_ERROR, D1_ERROR, TELEGRAM_ERROR, EXCEL_AGENT_ERROR, EXCEL_RECONCILIATION_ERROR, EXCEL_OFFLINE, EXCEL_RECOVERED, SYNC_BACKLOG, HEALTHCHECK_FAILED/RECOVERED, DEPLOYMENT_ERROR, and RUNNER_OFFLINE/RECOVERED.
