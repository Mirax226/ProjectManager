const test = require('node:test');
const assert = require('node:assert/strict');

const { maskDsn, tryFixPostgresDsn } = require('../configDb');
const { shouldRouteEvent, computeDestinations, shouldNotifyRecovery } = require('../opsReliability');

test('maskDsn masks password and preserves host/db', () => {
  const input = 'postgres://user:SuperSecret1234@db.example.com:5432/appdb?sslmode=require';
  const masked = maskDsn(input);
  assert.equal(masked.includes('SuperSecret1234'), false);
  assert.equal(masked.includes('***1234'), true);
  assert.equal(masked.includes('db.example.com:5432/appdb'), true);
});

test('tryFixPostgresDsn encodes credentials once', () => {
  const input = 'postgres://user:pa?ss@localhost:5432/demo';
  const result = tryFixPostgresDsn(input);
  assert.equal(result.fixed, true);
  assert.equal(result.dsn.includes('%3F'), true);
});

test('alert routing mutes non-reportable categories', () => {
  const prefs = {
    user_id: '1',
    enable_alerts: true,
    severity_threshold: 'warn',
    mute_categories: ['ENV_MISCONFIG'],
    rate_limits: { max_per_10m: 10, debounce_seconds_per_category: 0 },
    destinations: { admin_inbox: true, admin_room: true, admin_rob: true },
  };
  const event = { level: 'error', category: 'ENV_MISCONFIG' };
  assert.equal(shouldRouteEvent(prefs, event), false);
});

test('critical destination mapping includes admin_rob when enabled', () => {
  const prefs = { destinations: { admin_inbox: true, admin_room: true, admin_rob: true } };
  const destinations = computeDestinations(prefs, 'critical');
  assert.deepEqual(destinations, { admin_inbox: true, admin_room: true, admin_rob: true });
});


test('recovery notification debounce allows only one notification per outage', () => {
  assert.equal(shouldNotifyRecovery({ status: 'HEALTHY', lastOutageId: 2, lastRecoveryNotifiedOutageId: 1 }), true);
  assert.equal(shouldNotifyRecovery({ status: 'HEALTHY', lastOutageId: 2, lastRecoveryNotifiedOutageId: 2 }), false);
  assert.equal(shouldNotifyRecovery({ status: 'DOWN', lastOutageId: 2, lastRecoveryNotifiedOutageId: 1 }), false);
});
