const infrastructureCodes = new Set([
  '40P01', '40001', '55P03', '57014', '57P01', '57P02', '57P03',
  'ECONNREFUSED', 'ECONNRESET', 'ECONNABORTED', 'ETIMEDOUT', 'EPIPE',
  'ENOTFOUND', 'EAI_AGAIN', 'ENETUNREACH', 'EHOSTUNREACH',
  'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_SOCKET', 'SQS_CALL_TIMEOUT',
]);

export function errorChain(error: unknown): Record<string, unknown>[] {
  const chain: Record<string, unknown>[] = [];
  let current = error;
  for (let depth = 0; depth < 6; depth += 1) {
    if (current === null || typeof current !== 'object') break;
    const fields = current as Record<string, unknown>;
    chain.push(fields);
    current = fields.cause;
  }
  return chain;
}

export function isInfrastructureFailure(chain: Record<string, unknown>[]): boolean {
  return chain.some(fields => {
    if (typeof fields.code === 'string'
      && (infrastructureCodes.has(fields.code) || /^(08|53)[A-Z0-9]{3}$/.test(fields.code))) return true;
    if (fields.code === '55000' && fields.severity === 'FATAL') return true;
    return fields.name === 'KnexTimeoutError';
  });
}
