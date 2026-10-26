import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { createCrashEnvironment, createFinancialEvent, financialState, receiveEvents, type CrashEnvironment } from '../fixtures/crash-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('worker startup diagnostic failure', () => {
  let environment: CrashEnvironment;
  beforeAll(async () => { environment = await createCrashEnvironment('worker_diag'); });
  afterAll(async () => { await environment?.close(); });

  test('keeps publishing and shuts down cleanly when console.log rejects the startup event', async () => {
    const { eventId, walletId } = await createFinancialEvent(environment);
    const queueUrl = await environment.createQueue();
    const child = Bun.spawn([process.execPath, 'test/fixtures/worker-diagnostic-sink.ts'], {
      stdin: 'ignore', stdout: 'pipe', stderr: 'pipe',
      env: { ...process.env, DATABASE_URL: environment.databaseUrl, EVENT_QUEUE_URL: queueUrl,
        WORKER_ROLE: 'publisher', WORKER_METRICS_PORT: '0' },
    });
    const stdout = new Response(child.stdout).text();
    const stderr = new Response(child.stderr).text();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const deadline = performance.now() + 3_000;
      for (;;) {
        if (child.exitCode !== null) throw new Error(`Worker exited after a diagnostic failure: ${child.exitCode}`);
        const [event] = await environment.query<{ published: boolean }>('select published_at is not null as published from dwp.outbox_messages where id = ?', [eventId]);
        if (event?.published) break;
        if (performance.now() >= deadline) throw new Error('Worker did not publish its durable event');
        await Bun.sleep(20);
      }
      expect(child.exitCode).toBeNull();
      child.kill('SIGTERM');
      const exited = await Promise.race([child.exited, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Worker did not shut down')), 3_000);
      })]).finally(() => clearTimeout(timer));
      expect(exited).toBe(0);
      expect(await stderr).toBe('');
      expect(await stdout).not.toContain('worker_started');
      expect((await receiveEvents(environment.sqs, queueUrl)).map(event => event.eventId)).toEqual([eventId]);
      expect(await financialState(environment, walletId)).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
      await child.exited;
    }
  }, 8_000);
});
