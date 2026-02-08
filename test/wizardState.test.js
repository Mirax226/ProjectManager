const assert = require('node:assert/strict');
const test = require('node:test');

const { createWizardState, goNext, goBack, canGoBack } = require('../src/wizardState');

test('wizard supports back transitions and preserves inputs', () => {
  const state = createWizardState(['a', 'b', 'c']);
  assert.equal(state.currentStep, 'a');
  assert.equal(canGoBack(state), false);

  goNext(state, { token: 'x' });
  assert.equal(state.currentStep, 'b');
  assert.equal(canGoBack(state), true);

  goBack(state);
  assert.equal(state.currentStep, 'a');
  assert.equal(state.collectedInputs.token, 'x');
});
