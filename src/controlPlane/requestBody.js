const MAX_BODY = 32768;
async function boundedJson(request) {
  if (Number(request.headers.get('content-length')) > MAX_BODY) throw new Error('body_limit');
  const reader = request.body?.getReader(); if (!reader) throw new Error('invalid_body');
  const chunks = []; let size = 0;
  try {
    while (true) { const { value, done } = await reader.read(); if (done) break; size += value.length; if (size > MAX_BODY) { await reader.cancel(); throw new Error('body_limit'); } chunks.push(value); }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size); let at = 0; for (const chunk of chunks) { bytes.set(chunk, at); at += chunk.length; }
  return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
}
module.exports = { boundedJson, MAX_BODY };
