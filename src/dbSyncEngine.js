function resolveConflictLastWriteWins(leftRow, rightRow) {
  const leftTs = Date.parse(leftRow?.updated_at || 0) || 0;
  const rightTs = Date.parse(rightRow?.updated_at || 0) || 0;
  if (leftTs >= rightTs) return { ...leftRow };
  return { ...rightRow };
}

function batchRows(rows, size = 500) {
  const batchSize = Math.max(1, Number(size) || 500);
  const out = [];
  for (let i = 0; i < rows.length; i += batchSize) {
    out.push(rows.slice(i, i + batchSize));
  }
  return out;
}

async function syncPair({ sourceRows, targetRows, key = 'id' }) {
  const targetMap = new Map((targetRows || []).map((row) => [row[key], row]));
  const upserts = [];
  for (const row of sourceRows || []) {
    const existing = targetMap.get(row[key]);
    if (!existing) {
      upserts.push(row);
      continue;
    }
    const winner = resolveConflictLastWriteWins(row, existing);
    if (winner.updated_at !== existing.updated_at) {
      upserts.push(winner);
    }
  }
  return upserts;
}

module.exports = {
  resolveConflictLastWriteWins,
  batchRows,
  syncPair,
};
