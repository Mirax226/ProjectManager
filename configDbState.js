const state = {
  ready: false,
  lastErrorCategory: null,
  lastErrorMessage: null,
  lastAttemptAt: null,
  lastSuccessAt: null,
  attempts: 0,
  nextRetryInMs: 0,
};

function recordAttempt() {
  state.attempts += 1;
  state.lastAttemptAt = new Date().toISOString();
}

function setReady() {
  state.ready = true;
  state.lastErrorCategory = null;
  state.lastErrorMessage = null;
  state.lastSuccessAt = new Date().toISOString();
  state.nextRetryInMs = 0;
}

function setError(category, message) {
  state.ready = false;
  state.lastErrorCategory = category || 'UNKNOWN_DB_ERROR';
  state.lastErrorMessage = message || null;
}

function setNextRetryInMs(nextRetryInMs) {
  state.nextRetryInMs = Number.isFinite(nextRetryInMs) ? Math.max(0, nextRetryInMs) : 0;
}

function snapshot() {
  return { ...state };
}

module.exports = {
  recordAttempt,
  setReady,
  setError,
  setNextRetryInMs,
  snapshot,
};
