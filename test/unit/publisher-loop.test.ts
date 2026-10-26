import { expect, test } from 'bun:test';
import { publisherConfiguration, publisherDelay, runPublisherLoop } from '../../src/infrastructure/messaging/publisher-loop.js';

test('publisher configuration is bounded and defaults are independent of the load runner', () => {
  expect(publisherConfiguration({})).toEqual({ batchSize: 10, leaseMs: 30_000, idleMs: 250, errorMs: 1_000 });
  expect(publisherConfiguration({ OUTBOX_BATCH_SIZE: '100' }).batchSize).toBe(100);
  for (const raw of ['0', '101', '1.5', '-1', 'NaN', '']) {
    expect(() => publisherConfiguration({ OUTBOX_BATCH_SIZE: raw })).toThrow('OUTBOX_BATCH_SIZE');
  }
});

test('productive batches continue immediately and only empty/error cycles wait', async () => {
  const abort = new AbortController();
  const values: (number | Error)[] = [10, 3, 0, new Error('database unavailable'), 2];
  const published: number[] = [];
  const waits: number[] = [];
  let failures = 0;
  await runPublisherLoop({ async runOnce() {
    const next = values.shift();
    if (next instanceof Error) throw next;
    if (values.length === 0) abort.abort();
    return next!;
  } }, publisherConfiguration({}), {
    signal: abort.signal,
    onPublished: count => { published.push(count); },
    onError: () => { failures += 1; },
    wait: async milliseconds => { waits.push(milliseconds); },
  });
  expect(published).toEqual([10, 3, 2]);
  expect(waits).toEqual([250, 1_000]);
  expect(failures).toBe(1);
});

test('diagnostic failures cannot stop the publisher loop', async () => {
  const abort = new AbortController();
  let calls = 0;
  await runPublisherLoop({ async runOnce() {
    calls += 1;
    if (calls === 1) throw new Error('retry');
    abort.abort();
    return 1;
  } }, publisherConfiguration({}), {
    signal: abort.signal,
    onError: () => { throw new Error('diagnostics unavailable'); },
    onPublished: () => { throw new Error('diagnostics unavailable'); },
    wait: async () => undefined,
  });
  expect(calls).toBe(2);
});

test('an idle publisher wakes immediately when shutdown is requested', async () => {
  const abort = new AbortController();
  const waiting = publisherDelay(60_000, abort.signal);
  abort.abort();
  await waiting;
});
