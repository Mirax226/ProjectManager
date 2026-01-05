const { loadJson, saveJson } = require('./configStore');
const { forwardSelfLog } = require('./logger');

const SUPABASE_CONNECTIONS_KEY = 'supabaseConnections';

let cache = [];

async function loadSupabaseConnections() {
  const value = await loadJson(SUPABASE_CONNECTIONS_KEY);
  if (!value || !Array.isArray(value)) return [];
  cache = value;
  return cache;
}

async function saveSupabaseConnections(conns) {
  if (!Array.isArray(conns)) throw new Error('Supabase connections must be an array');
  try {
    cache = conns;
    await saveJson(SUPABASE_CONNECTIONS_KEY, conns);
  } catch (error) {
    console.error('[supabaseConnectionsStore] Failed to save supabase connections', error);
    await forwardSelfLog('error', 'Failed to save Supabase connections', {
      stack: error?.stack,
      context: { error: error?.message },
    });
    throw error;
  }
}

function findSupabaseConnection(id) {
  return cache.find((connection) => connection.id === id);
}

module.exports = {
  loadSupabaseConnections,
  findSupabaseConnection,
  saveSupabaseConnections,
};
