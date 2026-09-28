const http = require('http');
const { ControlPlaneStore } = require('./store');
const { createAuthenticator } = require('./auth');
const { createControlPlaneHandler } = require('./handler');

function startControlPlaneServer(options = {}) {
  const store = options.store || new ControlPlaneStore();
  const handler = createControlPlaneHandler({ store, authenticator: options.authenticator || createAuthenticator(options.tokens || {}) });
  const server = http.createServer(async (req, res) => {
    try {
      const body = await new Promise((resolve, reject) => { let value = ''; req.on('data', (chunk) => { value += chunk; if (value.length > 1024 * 1024) reject(new Error('payload too large')); }); req.on('end', () => resolve(value)); req.on('error', reject); });
      const request = new Request(`http://${req.headers.host || 'localhost'}${req.url}`, { method: req.method, headers: req.headers, body: ['GET', 'HEAD'].includes(req.method) ? undefined : body });
      const response = await handler(request); const text = await response.text(); res.writeHead(response.status, Object.fromEntries(response.headers.entries())); res.end(text);
    } catch (error) { res.writeHead(error.message === 'payload too large' ? 413 : 500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: false, error: 'internal_error' })); }
  });
  const port = Number(options.port || process.env.PG_CONTROL_PLANE_PORT || 8788);
  return new Promise((resolve) => server.listen(port, () => resolve({ server, store, port })));
}

if (require.main === module) startControlPlaneServer({ tokens: { adminToken: process.env.PG_CONTROL_PLANE_ADMIN_TOKEN || 'local-dev-only' } }).then(({ port }) => console.log(`PG control plane listening on ${port}`));
module.exports = { startControlPlaneServer };
