import { describe, expect, test } from 'bun:test';
import { text, wagerRequest, walletRequest } from '../../src/application/contracts.js';

function expectInvalidRequest(operation: () => unknown): void {
  let caught: unknown;
  try { operation(); } catch (error) { caught = error; }
  expect(caught).toMatchObject({ kind: 'validation', code: 'INVALID_REQUEST' });
}

describe('Unicode identity contracts', () => {
  test.each(['\ud800', '\udfff', 'before\ud800after', 'before\udfffafter', '\ud800\ud800', '\udfff\udfff'])(
    'rejects malformed UTF-16 identity %j before hashing or persistence', value => {
      expectInvalidRequest(() => text(value, 'externalTransactionId'));
      expectInvalidRequest(() => walletRequest({ playerId: value, initialBalance: { amount: '1.00', currency: 'BRL' } }));
    },
  );

  test('preserves valid surrogate pairs and Unicode normalization form exactly', () => {
    for (const value of ['player-😀', 'player-𐐀', 'player-e\u0301', 'player-é', 'player-�']) {
      expect(text(value, 'playerId')).toBe(value);
    }
    expect(text('e\u0301', 'id')).not.toBe(text('é', 'id'));
    expect(text('😀', 'id')).not.toBe('�');
  });

  test('validates malformed references instead of passing them to the canonical business payload', () => {
    const body = {
      providerId: 'provider', externalTransactionId: 'win', playerId: 'player',
      walletId: '00000000-0000-0000-0000-000000000001', roundId: 'round', gameId: 'game',
      kind: 'WIN', money: { amount: '1.00', currency: 'BRL' }, referenceExternalTransactionId: 'bet\ud800',
    };
    expectInvalidRequest(() => wagerRequest(body));
  });
});
