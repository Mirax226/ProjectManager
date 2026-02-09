const test = require('node:test');
const assert = require('node:assert/strict');
const { createSafeModeController } = require('../src/safeMode');

test('safe mode enters on restart loop and exits after stability window', () => {
  const sm = createSafeModeController({ restartThreshold: 3, restartWindowMs: 1000, stableExitMs: 1000 });
  sm.markRestart(0);
  sm.markRestart(100);
  const enter = sm.markRestart(200);
  assert.equal(enter.mode, 'on');
  const exit = sm.evaluate(1300);
  assert.equal(exit.mode, 'off');
});
