const { loadJson, saveJson } = require('./configStore');

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
  cache = conns;
  await saveJson(SUPABASE_CONNECTIONS_KEY, conns);
}

function findSupabaseConnection(id) {
  return cache.find((connection) => connection.id === id);
}

module.exports = {
  loadSupabaseConnections,
  findSupabaseConnection,
  saveSupabaseConnections,
};
