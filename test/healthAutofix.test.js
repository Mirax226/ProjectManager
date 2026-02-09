const test = require('node:test');
const assert = require('node:assert/strict');

const { __test } = require('../bot');

test('auto-fix health setup only fills missing fields', () => {
  const result = __test.applyHealthAutoFixDefaults({
    startCommand: 'custom start',
    testCommand: '',
    diagnosticCommand: null,
    healthPath: undefined,
    servicePort: '8080',
  });
  assert.equal(result.project.startCommand, 'custom start');
  assert.equal(result.project.testCommand, 'npm test');
  assert.equal(result.project.diagnosticCommand, 'node -e "console.log(\'PM diagnostic OK\')"');
  assert.equal(result.project.healthPath, '/health');
  assert.equal(result.project.servicePort, '8080');
});
