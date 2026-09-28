function value(input, fallback = 'UNKNOWN') { return String(input == null || input === '' ? fallback : input).slice(0, 120); }

function buildDailySystemOpsView(input = {}) {
  const project = value(input.project || input.projectId, 'daily-system');
  const lines = [`📊 Operations — ${project}`, ''];
  const rows = [
    ['Excel', input.excel],
    ['Runner', input.runner],
    ['Backup', input.backup],
    ['Sync', input.sync],
    ['Reconciliation', input.reconciliation],
  ];
  rows.forEach(([label, state]) => {
    const item = state && typeof state === 'object' ? state : {};
    lines.push(`${label}: ${value(item.status)}${item.lastSeenAt ? ` · ${value(item.lastSeenAt)}` : ''}${item.message ? ` · ${value(item.message, '-')}` : ''}`);
  });
  const providers = Array.isArray(input.providers) ? input.providers : [];
  if (providers.length) {
    lines.push('', 'Providers:');
    providers.slice(0, 10).forEach((provider) => lines.push(`• ${value(provider.name)} — ${value(provider.health)} · ${value(provider.status)}`));
  }
  return { text: lines.join('\n'), project };
}

module.exports = { buildDailySystemOpsView };
