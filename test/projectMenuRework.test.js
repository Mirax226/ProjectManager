const assert = require('node:assert/strict');
const test = require('node:test');

const { __test } = require('../bot');

function keyboardTexts(inline) {
  return (inline?.inline_keyboard || []).flat().map((btn) => btn.text);
}

function keyboardCallbacks(inline) {
  return (inline?.inline_keyboard || []).flat().map((btn) => btn.callback_data);
}

test('project hub includes working overview route and excludes access button', () => {
  const project = { id: 'demo', name: 'Demo Project' };
  const view = __test.buildProjectHubView(project);
  const texts = keyboardTexts(view.keyboard);
  const callbacks = keyboardCallbacks(view.keyboard);

  assert.ok(texts.includes('🧾 Overview'));
  assert.ok(callbacks.includes('proj:overview:demo'));
  assert.ok(!texts.some((text) => /Access/i.test(text)));
});

test('default actions keyboard shows clear default only for default project', () => {
  const defaultKb = __test.buildDefaultProjectActionsKeyboard('demo', true);
  const notDefaultKb = __test.buildDefaultProjectActionsKeyboard('demo', false);

  const defaultTexts = keyboardTexts(defaultKb);
  const notDefaultTexts = keyboardTexts(notDefaultKb);

  assert.ok(defaultTexts.includes('🧹 Clear default'));
  assert.ok(!notDefaultTexts.includes('🧹 Clear default'));
});
