const fs = require('fs/promises');
const path = require('path');

const CONNECTIONS_FILE = path.join(__dirname, 'supabaseConnections.json');
let cachedConnections;

async function loadSupabaseConnections() {
  if (cachedConnections) {
    return cachedConnections;
  }
  try {
    const raw = await fs.readFile(CONNECTIONS_FILE, 'utf-8');
    cachedConnections = JSON.parse(raw);
    return cachedConnections;
  } catch (error) {
    console.error('Failed to load supabaseConnections.json', error);
    cachedConnections = [];
    return cachedConnections;
  }
}

async function findSupabaseConnection(id) {
  const connections = await loadSupabaseConnections();
  return connections.find((conn) => conn.id === id);
}

module.exports = {
  loadSupabaseConnections,
  findSupabaseConnection,
};
