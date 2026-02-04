function sanitizeDbErrorMessage(message) {
  if (!message) return null;
  let sanitized = String(message);
  sanitized = sanitized.replace(/postgres(?:ql)?:\/\/[^@\s]+@/gi, 'postgres://***@');
  sanitized = sanitized.replace(/password=([^\s]+)/gi, 'password=***');
  sanitized = sanitized.replace(/token=([^\s]+)/gi, 'token=***');
  sanitized = sanitized.replace(/key=([^\s]+)/gi, 'key=***');
  return sanitized;
}

function classifyDbError(error) {
  const code = error?.code;
  const message = String(error?.message || '');
  const lower = message.toLowerCase();

  if (
    code === 'ETIMEDOUT' ||
    code === 'DB_TIMEOUT' ||
    lower.includes('timeout') ||
    lower.includes('timed out')
  ) {
    return 'DB_TIMEOUT';
  }

  if (code === 'ECONNRESET' || lower.includes('connection terminated unexpectedly')) {
    return 'CONNECTION_TERMINATED_UNEXPECTEDLY';
  }

  if (code === '28P01' || lower.includes('password authentication failed') || lower.includes('authentication failed')) {
    return 'AUTH_FAILED';
  }

  if (code === 'ENOTFOUND' || code === 'EAI_AGAIN' || lower.includes('getaddrinfo')) {
    return 'DNS_FAILED';
  }

  if (
    code === 'SELF_SIGNED_CERT_IN_CHAIN' ||
    code === 'DEPTH_ZERO_SELF_SIGNED_CERT' ||
    lower.includes('ssl') ||
    lower.includes('certificate')
  ) {
    return 'SSL_ERROR';
  }

  return 'UNKNOWN_DB_ERROR';
}

module.exports = {
  classifyDbError,
  sanitizeDbErrorMessage,
};
