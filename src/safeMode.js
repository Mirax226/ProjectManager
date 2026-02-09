function createSafeModeController(options = {}) {
  const cfg = {
    restartWindowMs: Number(options.restartWindowMs) || 10 * 60 * 1000,
    restartThreshold: Number(options.restartThreshold) || 3,
    dbDownMs: Number(options.dbDownMs) || 3 * 60 * 1000,
    errorWindowMs: Number(options.errorWindowMs) || 5 * 60 * 1000,
    errorThreshold: Number(options.errorThreshold) || 12,
    heapMbThreshold: Number(options.heapMbThreshold) || 700,
    stableExitMs: Number(options.stableExitMs) || 5 * 60 * 1000,
  };
  const state = {
    mode: 'off',
    forced: null,
    lastEnteredReason: null,
    enteredAt: null,
    restarts: [],
    dbDownSince: null,
    errors: [],
    lastTriggerAt: null,
  };

  function prune(now) {
    state.restarts = state.restarts.filter((ts) => now - ts <= cfg.restartWindowMs);
    state.errors = state.errors.filter((ts) => now - ts <= cfg.errorWindowMs);
  }

  function evaluate(now = Date.now(), heapUsedMb = null) {
    prune(now);
    const reasons = [];
    if (state.restarts.length >= cfg.restartThreshold) reasons.push('restart_loop');
    if (state.dbDownSince && now - state.dbDownSince >= cfg.dbDownMs) reasons.push('db_down');
    if (state.errors.length >= cfg.errorThreshold) reasons.push('error_spike');
    if (heapUsedMb != null && heapUsedMb >= cfg.heapMbThreshold) reasons.push('memory_pressure');

    if (state.forced === 'on') {
      state.mode = 'on';
      return { changed: false, mode: state.mode, reasons: ['forced_on'] };
    }
    if (state.forced === 'off') {
      state.mode = 'off';
      return { changed: false, mode: state.mode, reasons: ['forced_off'] };
    }

    if (reasons.length) {
      state.lastTriggerAt = now;
      if (state.mode !== 'on') {
        state.mode = 'on';
        state.enteredAt = now;
        state.lastEnteredReason = reasons[0];
        return { changed: true, mode: state.mode, reasons };
      }
      return { changed: false, mode: state.mode, reasons };
    }

    if (state.mode === 'on' && state.lastTriggerAt && now - state.lastTriggerAt >= cfg.stableExitMs) {
      state.mode = 'off';
      return { changed: true, mode: state.mode, reasons: ['stable_recovery'] };
    }
    return { changed: false, mode: state.mode, reasons: [] };
  }

  return {
    markRestart(now = Date.now()) { state.restarts.push(now); return evaluate(now); },
    markDbStatus(status, now = Date.now()) {
      if (String(status).toUpperCase() === 'DOWN') {
        state.dbDownSince = state.dbDownSince || now;
      } else {
        state.dbDownSince = null;
      }
      return evaluate(now);
    },
    markSeverity(severity, now = Date.now()) {
      if (['error', 'critical'].includes(String(severity).toLowerCase())) state.errors.push(now);
      return evaluate(now);
    },
    setForced(mode) { state.forced = mode === 'on' || mode === 'off' ? mode : null; return evaluate(Date.now()); },
    resetCounters() { state.restarts = []; state.errors = []; state.dbDownSince = null; state.lastTriggerAt = null; },
    getStatus() { return { ...state }; },
    evaluate,
  };
}

module.exports = { createSafeModeController };
