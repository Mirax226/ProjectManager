module.exports = {
  id: 'SUPABASE_FETCH_FAILED',
  title: 'Supabase fetch failed in user lookup path',
  triggers: ['fetch failed', 'supabase', 'handlesupabaseerror', 'getuserbytelegramid'],
  match(ctx) {
    const message = String(ctx.normalizedMessage || ctx.normalizedText || '');
    const stack = String(ctx.normalizedStack || ctx.normalizedText || '');
    const category = String(ctx.category || '').toUpperCase();
    const hasFetchFailed = message.includes('fetch failed') || stack.includes('fetch failed');
    const hasSupabaseSignal = stack.includes('supabase')
      || stack.includes('handlesupabaseerror')
      || stack.includes('getuserbytelegramid')
      || message.includes('supabase');
    if (!hasFetchFailed || !hasSupabaseSignal) return null;
    let confidence = 0.92;
    if (category === 'FASTIFY_REQUEST_ERROR' || category === 'SUPABASE_FETCH_FAILED') confidence += 0.03;
    return { confidence: Math.min(confidence, 0.99), fields: {} };
  },
  render() {
    return {
      diagnosis: [
        'Supabase call failed at network/request layer before a valid application response.',
        'User lookup path (handleSupabaseError/getUserByTelegramId) is not resilient to fetch/transient failures.',
      ],
      steps: [
        'Verify SUPABASE_URL and key configuration (SUPABASE_ANON_KEY or SUPABASE_SERVICE_ROLE_KEY) for this deployment.',
        'Check network egress/DNS/TLS reachability from the runtime to Supabase, then redeploy after config fixes.',
        'If this incident is active, keep responses graceful for user-facing paths and queue retry for failed user fetches.',
      ],
      task:
        'Harden Supabase client calls in client repo: (1) add request timeout + bounded retry with exponential backoff, (2) classify and log failure types (DNS, timeout, 401/403, 429, 5xx) with correlationId, (3) implement graceful fallback when user fetch fails (respond safely + queue retry), (4) add tests covering transient fetch failed and auth/rate-limit branches.',
    };
  },
};
