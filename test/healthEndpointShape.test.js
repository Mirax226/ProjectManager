const test = require('node:test');
const assert = require('node:assert/strict');

const { __test } = require('../bot');

test('health payload masking helper does not leak tokens', () => {
  const masked = __test.maskDiagnosticText('token=abc123 SECRET_VALUE');
  assert.doesNotMatch(masked, /abc123/);
});
