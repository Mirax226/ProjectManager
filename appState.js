const { setError: setConfigDbError, setReady: setConfigDbReady } = require('./configDbState');
const { sanitizeDbErrorMessage } = require('./configDbErrors');

const appState = {
  dbReady: false,
  degradedMode: false,
  lastDbError: null,
};

function setDbReady(ready) {
  appState.dbReady = Boolean(ready);
  if (ready) {
    setConfigDbReady();
    appState.lastDbError = null;
    appState.degradedMode = false;
  }
}

function setDegradedMode(degraded) {
  appState.degradedMode = Boolean(degraded);
}

function recordDbError(category, message) {
  if (!category) return;
  const sanitizedMessage = sanitizeDbErrorMessage(message) || null;
  appState.dbReady = false;
  appState.degradedMode = true;
  appState.lastDbError = {
    category,
    message: sanitizedMessage,
    at: new Date().toISOString(),
  };
  setConfigDbError(category, sanitizedMessage);
}

module.exports = {
  appState,
  setDbReady,
  setDegradedMode,
  recordDbError,
};
