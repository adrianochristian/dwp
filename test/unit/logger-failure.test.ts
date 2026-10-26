import { expect, spyOn, test } from 'bun:test';
import { log } from '../../src/observability/logger.js';

test('an unavailable log sink cannot turn a completed operation into an error', () => {
  const output = spyOn(console, 'log').mockImplementation(() => { throw new Error('sink unavailable'); });
  try {
    expect(() => log('transaction_completed', { transactionId: 'committed-operation' })).not.toThrow();
  } finally { output.mockRestore(); }
});
