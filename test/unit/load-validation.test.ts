import { expect, test } from 'bun:test';
import { validateTraffic } from '../../scripts/load.js';

const success = { requests: 1, successful: 1, errors: 0, unexpectedReplays: 0 };
const noWarmup = { requests: 0, successful: 0, errors: 0, unexpectedReplays: 0 };

test('a reconciled load experiment still fails if valid requests never processed', () => {
  const failures = Array.from({ length: 6 }, () => ({ requests: 1, successful: 0, errors: 1, unexpectedReplays: 0 }));
  expect(validateTraffic(failures, noWarmup, '0').consistent).toBe(false);
});

test('a complete load accounts for every measured and warmup bet without a throughput target', () => {
  const result = validateTraffic(Array(6).fill(success), success, '7');
  expect(result.consistent).toBe(true);
  expect(result.expectedBets).toBe(7);
  expect(result.measured.successful).toBe(6);
  expect(validateTraffic(Array(6).fill(success), noWarmup, '6').consistent).toBe(true);
});

test('replay, warmup errors, missing windows and unexpected persisted bets invalidate the experiment', () => {
  expect(validateTraffic([{ ...success, unexpectedReplays: 1 }, ...Array(5).fill(success)], noWarmup, '6').consistent).toBe(false);
  expect(validateTraffic(Array(6).fill(success), { ...noWarmup, requests: 1, errors: 1 }, '6').consistent).toBe(false);
  expect(validateTraffic(Array(5).fill(success), noWarmup, '5').consistent).toBe(false);
  expect(validateTraffic(Array(6).fill(success), noWarmup, '7').consistent).toBe(false);
  expect(validateTraffic([noWarmup, ...Array(5).fill(success)], noWarmup, '5').consistent).toBe(false);
});
