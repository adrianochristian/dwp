import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { createCrashEnvironment, createFinancialEvent, financialState, type CrashEnvironment } from '../fixtures/crash-support.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('worker shutdown watchdog with unavailable dependencies', () => {
  let environment: CrashEnvironment;
  beforeAll(async () => { environment = await createCrashEnvironment('worker_deadline'); });
  afterAll(async () => { await environment?.close(); });

  test('exits nonzero on its deadline while SQL cleanup is blocked and retains the event', async () => {
    const { eventId, walletId } = await createFinancialEvent(environment);
    let received!: () => void;
    const entered = new Promise<void>(resolve => { received = resolve; });
    const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: async () => {
      received();
      return new Promise<Response>(() => undefined);
    } });
    const child = Bun.spawn([process.execPath, 'test/fixtures/worker-deadline.ts'], {
      stdin: 'ignore', stdout: 'pipe', stderr: 'pipe',
      env: { ...process.env, DATABASE_URL: environment.databaseUrl, SQS_ENDPOINT: server.url.href,
        EVENT_QUEUE_URL: server.url.href, WORKER_ROLE: 'publisher', WORKER_METRICS_PORT: '0' },
    });
    const stdout = new Response(child.stdout).text();
    const stderr = new Response(child.stderr).text();
    let release!: () => void;
    let holder: Promise<void> | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([entered, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Worker did not begin publishing')), 3_000);
      })]).finally(() => clearTimeout(timer));
      let acquired!: () => void;
      const locked = new Promise<void>(resolve => { acquired = resolve; });
      const blocked = new Promise<void>(resolve => { release = resolve; });
      holder = environment.orm.em.fork().transactional(async em => {
        await em.execute('select id from dwp.outbox_messages where id = ? for update', [eventId]);
        acquired();
        await blocked;
      });
      await locked;
      const started = performance.now();
      child.kill('SIGTERM');
      const exitCode = await Promise.race([child.exited, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Worker exceeded its shutdown deadline')), 2_000);
      })]).finally(() => clearTimeout(timer));
      expect(exitCode).toBe(1);
      expect(performance.now() - started).toBeLessThan(1_500);
      const logs = (await stdout).trim().split('\n').map(line => JSON.parse(line) as { event: string });
      expect(logs.some(line => line.event === 'worker_started')).toBe(true);
      expect(logs.some(line => line.event === 'worker_shutdown_deadline')).toBe(true);
      expect(await stderr).toBe('');
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
      await child.exited;
      release?.();
      await holder;
      await server.stop(true);
    }
    expect((await environment.query<{ published_at: Date | null; event_id: string }>(
      "select published_at, payload->>'eventId' as event_id from dwp.outbox_messages where id = ?", [eventId],
    ))[0]).toEqual({ published_at: null, event_id: eventId });
    expect(await financialState(environment, walletId)).toEqual({ balance_cents: '7500', calculated: '7500', debits: '1' });
  }, 8_000);
});
