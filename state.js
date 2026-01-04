const userState = new Map();

function setUserState(userId, state) {
  if (!userId) return;
  userState.set(String(userId), state);
}

function getUserState(userId) {
  if (!userId) return undefined;
  return userState.get(String(userId));
}

function clearUserState(userId) {
  if (!userId) return;
  userState.delete(String(userId));
}

module.exports = {
  setUserState,
  getUserState,
  clearUserState,
};
