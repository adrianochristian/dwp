import { expect, spyOn, test } from 'bun:test';
import type { WageringUseCases } from '../../src/application/wagering-service.js';
import { ObservedWageringService } from '../../src/observability/observed-wagering.js';

test('a failed reference retry retains the known transaction and correlation identifiers', async () => {
  const transactionId = crypto.randomUUID();
  const failure = Object.assign(new Error('private database details'), { code: 'ECONNRESET' });
  const inner: WageringUseCases = {
    async createWallet() { throw failure; }, async getWallet() { throw failure; },
    async submit() { throw failure; }, async failSubmission() { throw failure; }, async reprocess() { throw failure; },
  };
  const service = new ObservedWageringService(inner, 'reference');
  const output = spyOn(console, 'log').mockImplementation(() => {});
  try {
    await expect(service.reprocess(transactionId)).rejects.toBe(failure);
    const record = JSON.parse(output.mock.calls[0]![0] as string);
    expect(record).toMatchObject({ event: 'wager_transaction_error', transactionId, correlationId: transactionId, code: 'INFRASTRUCTURE_UNAVAILABLE' });
    expect(JSON.stringify(record)).not.toContain('private');
  } finally { output.mockRestore(); }
});
