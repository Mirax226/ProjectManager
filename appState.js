const appState = {
  dbReady: false,
  degradedMode: false,
  lastDbError: null,
};

function setDbReady(ready) {
  appState.dbReady = Boolean(ready);
  if (ready) {
    appState.lastDbError = null;
    appState.degradedMode = false;
  }
}

function setDegradedMode(degraded) {
  appState.degradedMode = Boolean(degraded);
}

function recordDbError(category, message) {
  if (!category) return;
  appState.dbReady = false;
  appState.degradedMode = true;
  appState.lastDbError = {
    category,
    message: message || null,
    at: new Date().toISOString(),
  };
}

module.exports = {
  appState,
  setDbReady,
  setDegradedMode,
  recordDbError,
};
