const test = require('node:test');
const assert = require('node:assert/strict');
const { ingestLog, listLogs, setLogStatus, autoResolveLogs, buildFingerprint } = require('../src/logsHubStore');
const { saveJson } = require('../configStore');

test('logs hub deduplicates by fingerprint while open', async () => {
  await saveJson('ops_event_log', []);
  const fp = buildFingerprint({ category: 'DB_ERROR', messageShort: 'db down', projectId: 'p1' });
  const a = await ingestLog({ projectId: 'p1', level: 'error', category: 'DB_ERROR', message_short: 'db down' });
  const b = await ingestLog({ projectId: 'p1', level: 'error', category: 'DB_ERROR', message_short: 'db down' });
  assert.equal(a.event.fingerprint, fp);
  assert.equal(b.created, false);
  assert.equal(b.event.occurrence_count, 2);
  const rows = await listLogs({ projectId: 'p1' });
  assert.equal(rows.length, 1);
});

test('resolved fingerprint creates new row on next occurrence', async () => {
  await saveJson('ops_event_log', []);
  const first = await ingestLog({ projectId: 'p2', level: 'error', category: 'CRON_ERROR', message_short: 'cron failed' });
  await setLogStatus({ fingerprint: first.event.fingerprint, status: 'resolved' });
  const second = await ingestLog({ projectId: 'p2', level: 'error', category: 'CRON_ERROR', message_short: 'cron failed' });
  assert.equal(second.created, true);
  const rows = await listLogs({ projectId: 'p2' });
  assert.equal(rows.length, 2);
  assert.equal(rows.filter((r) => r.status === 'resolved').length, 1);
});

test('auto resolve closes stale open events', async () => {
  await saveJson('ops_event_log', []);
  const inserted = await ingestLog({ projectId: 'p3', level: 'warn', category: 'DRIFT_DETECTED', message_short: 'drift' });
  const now = new Date(inserted.event.last_seen_at).getTime() + (11 * 60 * 1000);
  const result = await autoResolveLogs({ now, enabled: true });
  assert.equal(result.changed, 1);
  const rows = await listLogs({ projectId: 'p3' });
  assert.equal(rows[0].status, 'resolved');
});
