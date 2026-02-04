const appState = {
  dbReady: false,
  degradedMode: false,
  lastDbError: null,
};

function setDbReady(ready) {
  appState.dbReady = Boolean(ready);
  if (ready) {
    appState.lastDbError = null;
  }
}

function setDegradedMode(degraded) {
  appState.degradedMode = Boolean(degraded);
}

function recordDbError(category) {
  if (!category) return;
  appState.dbReady = false;
  appState.lastDbError = {
    category,
    at: new Date().toISOString(),
  };
}

module.exports = {
  appState,
  setDbReady,
  setDegradedMode,
  recordDbError,
};
