const assert = require('node:assert/strict');
const test = require('node:test');

const {
  navigateTo,
  parseStartPayload,
  handleGlobalCommand,
  handleStartCommand,
  handleReplyKeyboardNavigation,
  __test,
} = require('../bot');

function createMockApi() {
  const calls = {
    deleteMessage: [],
    editMessageText: [],
    editMessageReplyMarkup: [],
    sendMessage: [],
  };
  const api = {
    deleteMessage: async (...args) => {
      calls.deleteMessage.push(args);
    },
    editMessageText: async (...args) => {
      calls.editMessageText.push(args);
    },
    editMessageReplyMarkup: async (...args) => {
      calls.editMessageReplyMarkup.push(args);
    },
    sendMessage: async (...args) => {
      calls.sendMessage.push(args);
      return { chat: { id: args[0] }, message_id: 200 };
    },
  };
  return { api, calls };
}

test('parseStartPayload maps payloads and falls back to main', () => {
  assert.equal(parseStartPayload('logs'), 'logs');
  assert.equal(parseStartPayload('db'), 'database');
  assert.equal(parseStartPayload('unknown'), 'main');
  assert.equal(parseStartPayload(''), 'main');
});

test('navigateTo dispatches to route handlers', async () => {
  const called = [];
  const handlers = {
    main: async () => called.push('main'),
    logs: async () => called.push('logs'),
    database: async () => called.push('database'),
    cronjobs: async () => called.push('cronjobs'),
    settings: async () => called.push('settings'),
    projects: async () => called.push('projects'),
    deploy: async () => called.push('deploy'),
  };

  await navigateTo(1, 2, 'logs', { handlers });
  await navigateTo(1, 2, 'database', { handlers });
  await navigateTo(1, 2, 'unknown', { handlers });

  assert.deepEqual(called, ['logs', 'database', 'main']);
});

test('slash command deletes message and edits active menu', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(1);
  __test.setActivePanelMessageId(1, 42);

  const ctx = {
    chat: { id: 1 },
    message: { chat: { id: 1 }, message_id: 111, text: '/logs' },
    from: { id: 2 },
    api,
  };

  const handled = await handleGlobalCommand(ctx, '/logs', '');

  assert.equal(handled, true);
  assert.equal(calls.deleteMessage.length, 1);
  assert.deepEqual(calls.deleteMessage[0], [1, 111]);
  assert.equal(calls.editMessageText.length, 1);
  assert.equal(calls.editMessageText[0][0], 1);
  assert.equal(calls.editMessageText[0][1], 42);
  assert.equal(calls.sendMessage.length, 0);
});

test('reply keyboard tap deletes user message and keeps active menu', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(10);
  __test.setActivePanelMessageId(10, 99);

  const ctx = {
    chat: { id: 10 },
    message: { chat: { id: 10 }, message_id: 120, text: '📣 Logs' },
    from: { id: 77 },
    api,
  };

  await handleReplyKeyboardNavigation(ctx, 'logs');

  assert.equal(calls.deleteMessage.length, 1);
  assert.deepEqual(calls.deleteMessage[0], [10, 120]);
  assert.equal(calls.editMessageText.length, 1);
  assert.equal(calls.editMessageText[0][1], 99);
});

test('/start with payload cleans menus and routes', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(5);
  __test.setPanelHistoryForChat(5, [55]);
  __test.setActivePanelMessageId(5, 55);

  const ctx = {
    chat: { id: 5 },
    message: { chat: { id: 5 }, message_id: 222, text: '/start logs' },
    from: { id: 9 },
    api,
  };

  await handleStartCommand(ctx, 'logs');

  assert.equal(calls.deleteMessage.length, 2);
  assert.ok(calls.deleteMessage.some((entry) => entry[0] === 5 && entry[1] === 222));
  assert.ok(calls.deleteMessage.some((entry) => entry[0] === 5 && entry[1] === 55));
  assert.equal(calls.sendMessage.length, 1);
  assert.equal(__test.getActivePanelMessageId(5), 200);
});

test('/start without payload routes to main and cleans menus', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(6);
  __test.setPanelHistoryForChat(6, [66]);
  __test.setActivePanelMessageId(6, 66);

  const ctx = {
    chat: { id: 6 },
    message: { chat: { id: 6 }, message_id: 333, text: '/start' },
    from: { id: 10 },
    api,
  };

  await handleStartCommand(ctx, '');

  assert.equal(calls.deleteMessage.length, 2);
  assert.ok(calls.deleteMessage.some((entry) => entry[0] === 6 && entry[1] === 333));
  assert.ok(calls.deleteMessage.some((entry) => entry[0] === 6 && entry[1] === 66));
  assert.equal(calls.sendMessage.length, 1);
  assert.equal(__test.getActivePanelMessageId(6), 200);
});

test('navigation queues during operation in progress', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(20);
  __test.setNavigationOperationInProgress(20, true);
  __test.setActivePanelMessageId(20, 500);

  const ctx = {
    chat: { id: 20 },
    message: { chat: { id: 20 }, message_id: 401, text: '/logs' },
    from: { id: 88 },
    api,
  };

  await handleGlobalCommand(ctx, '/logs', '');

  assert.equal(calls.deleteMessage.length, 1);
  assert.equal(calls.editMessageText.length, 0);
  assert.equal(calls.sendMessage.length, 1);
});


test('global to project database drill-down keeps navigation callbacks', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(30);
  __test.setActivePanelMessageId(30, 300);

  const ctx = {
    chat: { id: 30 },
    message: { chat: { id: 30 }, message_id: 501, text: '/database' },
    from: { id: 90 },
    api,
  };

  const handled = await handleGlobalCommand(ctx, '/database', '');
  assert.equal(handled, true);
  assert.equal(calls.editMessageText.length, 1);
  const [, , renderedText] = calls.editMessageText[0];
  assert.match(renderedText, /Database/);
});

test('project scoped header renders only 3-line project header', () => {
  const header = __test.buildScopedHeader('PROJECT: Alpha', 'Main → Databases → Alpha → Database');
  assert.match(header, /🧩 Project: Alpha/);
  assert.match(header, /🆔 ID: Alpha/);
  assert.match(header, /📦 Scope: Project/);
  assert.doesNotMatch(header, /Breadcrumb:/);
});

test('project hub view renders single project header block without breadcrumb', () => {
  const view = __test.buildProjectHubView({ id: 'DailyManager', name: 'Daily Manager' });
  const body = view.text;
  assert.equal((body.match(/🧩 Project:/g) || []).length, 1);
  assert.equal((body.match(/🆔 ID:/g) || []).length, 1);
  assert.equal((body.match(/📦 Scope: Project/g) || []).length, 1);
  assert.doesNotMatch(body, /Breadcrumb:/);
});

test('delete callback deletes exact targeted message and answers callback first', async () => {
  const calls = { answer: 0, deleteMessage: [] };
  const ctx = {
    from: { id: 99 },
    callbackQuery: { id: 'cb-delete' },
    answerCallbackQuery: async () => {
      calls.answer += 1;
    },
    telegram: {
      deleteMessage: async (chatId, messageId) => {
        calls.deleteMessage.push([chatId, messageId]);
      },
    },
  };
  const result = await __test.handleDeleteMessageCallback(ctx, 'msg:delete:123:456');
  assert.equal(calls.answer, 1);
  assert.deepEqual(calls.deleteMessage, [[123, 456]]);
  assert.equal(result.ok, true);
  assert.equal(result.mode, 'deleted');
});

test('delete callback treats not-found as success', async () => {
  const ctx = {
    callbackQuery: { id: 'cb-delete' },
    answerCallbackQuery: async () => {},
    telegram: {
      deleteMessage: async () => {
        const error = new Error('Bad Request: message to delete not found');
        throw error;
      },
    },
  };
  const result = await __test.handleDeleteMessageCallback(ctx, 'msg:delete:123:456');
  assert.equal(result.ok, true);
  assert.equal(result.mode, 'already_deleted');
});

const { pushSnapshot, clearStack } = require('../navigationStackStore');

test('global db -> project db -> back returns to global db list', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(31);
  await clearStack(31, 91);

  const ctx = {
    chat: { id: 31 },
    callbackQuery: { data: 'nav:back', message: { chat: { id: 31 }, message_id: 777 } },
    from: { id: 91 },
    api,
    answerCallbackQuery: async () => {},
  };

  await pushSnapshot(31, 91, { routeId: 'cb:dbmenu:list', timestamp: Date.now() - 1000 });
  await pushSnapshot(31, 91, { routeId: 'cb:dbmenu:open:demo', timestamp: Date.now() });

  await __test.goBack(ctx);

  const renderedText = calls.editMessageText.length
    ? calls.editMessageText[0][2]
    : calls.sendMessage[0][1];
  assert.match(renderedText, /Database/);
});

test('goBack falls back to home when stack empty', async () => {
  const { api, calls } = createMockApi();
  __test.setBotApiForTests(api);
  __test.resetNavigationState(32);
  await clearStack(32, 92);

  const ctx = {
    chat: { id: 32 },
    from: { id: 92 },
    api,
  };

  await __test.goBack(ctx);

  const homeText = calls.editMessageText.length ? calls.editMessageText[0][2] : calls.sendMessage[0][1];
  assert.match(homeText, /Main menu/i);
});
