export interface LogContext {
  correlationId?: string | undefined;
  messageId?: string | undefined;
  transactionId?: string | undefined;
  walletId?: string | undefined;
  providerId?: string | undefined;
  code?: string | undefined;
  status?: string | undefined;
  attempt?: number | undefined;
  durationMs?: number | undefined;
}

const stringFields = ['correlationId', 'messageId', 'transactionId', 'walletId', 'providerId', 'code', 'status'] as const;
const numberFields = ['attempt', 'durationMs'] as const;

export function log(event: string, context: LogContext = {}): void {
  try {
    const record: Record<string, string | number> = { timestamp: new Date().toISOString(), event };
    for (const key of stringFields) {
      const value = context[key];
      if (typeof value === 'string') record[key] = value;
    }
    for (const key of numberFields) {
      const value = context[key];
      if (typeof value === 'number' && Number.isFinite(value)) record[key] = value;
    }
    console.log(JSON.stringify(record));
  } catch { /* An unavailable diagnostic sink cannot change an operation's outcome. */ }
}
