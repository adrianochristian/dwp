import { describe, expect, spyOn, test } from 'bun:test';
import { Metrics } from '../../src/observability/metrics.js';
import { log } from '../../src/observability/logger.js';

describe('operational metrics', () => {
  test('bounds cardinality and omits arbitrary identifiers and invalid label values', () => {
    const metrics = new Metrics();
    metrics.increment('transactions', { status: 'PROCESSED', role: 'http', walletId: 'private-wallet' } as never);
    metrics.increment('transactions', { status: 'PROCESSED', role: 'http', walletId: 'another-wallet' } as never);
    metrics.increment('duplicates', { role: 'arbitrary-player-id' });
    const output = metrics.renderPrometheus();
    expect(output).toContain('dwp_transactions_total{status="PROCESSED",role="http"} 2');
    expect(output).toContain('dwp_duplicates_total 1');
    expect(output).not.toContain('wallet');
    expect(output).not.toContain('arbitrary-player-id');
  });

  test('exports cumulative histogram buckets and ignores invalid observations', () => {
    const metrics = new Metrics();
    metrics.observeDuration(0.02, { role: 'consumer' });
    metrics.observeDuration(0.5, { role: 'consumer' });
    metrics.observeDuration(Number.NaN, { role: 'consumer' });
    metrics.observeDuration(-1, { role: 'consumer' });
    metrics.setOutboxLag(3.5);
    metrics.setOutboxLag(Number.POSITIVE_INFINITY);
    const output = metrics.renderPrometheus();
    expect(output).toContain('dwp_processing_duration_seconds_bucket{role="consumer",le="0.01"} 0');
    expect(output).toContain('dwp_processing_duration_seconds_bucket{role="consumer",le="0.025"} 1');
    expect(output).toContain('dwp_processing_duration_seconds_bucket{role="consumer",le="0.5"} 2');
    expect(output).toContain('dwp_processing_duration_seconds_bucket{role="consumer",le="+Inf"} 2');
    expect(output).toContain('dwp_processing_duration_seconds_count{role="consumer"} 2');
    expect(output).toContain('dwp_processing_duration_seconds_sum{role="consumer"} 0.52');
    expect(output).toContain('dwp_outbox_lag_seconds 3.5');
  });

  test('logs curated identifiers and operational fields while dropping payloads', () => {
    const output = spyOn(console, 'log').mockImplementation(() => {});
    try {
      const context = { walletId: 'wallet-1', status: 'PROCESSED', durationMs: 3, payload: { amount: '5000.00' }, token: 'secret' };
      log('transaction_processed', context);
      const line = output.mock.calls[0]?.[0];
      expect(typeof line).toBe('string');
      const record: Record<string, unknown> = JSON.parse(line as string);
      expect(record).toMatchObject({ event: 'transaction_processed', walletId: 'wallet-1', status: 'PROCESSED', durationMs: 3 });
      expect(record).not.toHaveProperty('payload');
      expect(record).not.toHaveProperty('token');
      expect(line).not.toContain('5000.00');
    } finally {
      output.mockRestore();
    }
  });
});
