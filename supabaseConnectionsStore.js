// src/supabaseConnectionsStore.js
// ساده شده: فقط از supabaseConnections.json می‌خوانیم
// و در حافظه cache می‌کنیم. هیچ دسترسی‌ای به Postgres ندارد.

const fs = require("fs");
const path = require("path");

let cachedConnections = null;
let loaded = false;

/**
 * SupabaseConnection:
 * {
 *   id: string;
 *   name: string;
 *   envKey: string;
 * }
 */

async function loadSupabaseConnections() {
  if (loaded && Array.isArray(cachedConnections)) {
    return cachedConnections;
  }

  const filePath = path.join(__dirname, "..", "supabaseConnections.json");

  try {
    const raw = await fs.promises.readFile(filePath, "utf8");
    const parsed = JSON.parse(raw);

    if (!Array.isArray(parsed)) {
      throw new Error("supabaseConnections.json must contain a JSON array");
    }

    cachedConnections = parsed;
    loaded = true;
  } catch (err) {
    console.error("Failed to load supabaseConnections.json:", err);
    cachedConnections = [];
    loaded = true;
  }

  return cachedConnections;
}

function findSupabaseConnection(id) {
  if (!Array.isArray(cachedConnections)) return undefined;
  return cachedConnections.find((c) => c.id === id);
}

// برای سازگاری با کد فعلی؛ فعلاً فقط cache را به‌روز می‌کنیم.
async function saveSupabaseConnections(connections) {
  if (Array.isArray(connections)) {
    cachedConnections = connections;
    loaded = true;
  }
}

module.exports = {
  loadSupabaseConnections,
  findSupabaseConnection,
  saveSupabaseConnections,
};
