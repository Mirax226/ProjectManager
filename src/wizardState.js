function createWizardState(steps, initial = {}) {
  return {
    steps: Array.isArray(steps) ? [...steps] : [],
    currentStep: initial.currentStep || steps?.[0] || null,
    collectedInputs: { ...(initial.collectedInputs || {}) },
    messageId: initial.messageId || null,
    chatId: initial.chatId || null,
    userId: initial.userId || null,
  };
}

function getStepIndex(state) {
  return state.steps.indexOf(state.currentStep);
}

function canGoBack(state) {
  return getStepIndex(state) > 0;
}

function goNext(state, updates = {}) {
  state.collectedInputs = { ...state.collectedInputs, ...updates };
  const idx = getStepIndex(state);
  state.currentStep = state.steps[idx + 1] || null;
  return state.currentStep;
}

function goBack(state) {
  const idx = getStepIndex(state);
  if (idx <= 0) return state.currentStep;
  state.currentStep = state.steps[idx - 1];
  return state.currentStep;
}

module.exports = {
  createWizardState,
  canGoBack,
  goNext,
  goBack,
};
