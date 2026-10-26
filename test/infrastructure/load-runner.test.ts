import { expect, test } from 'bun:test';
import { rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const integration = process.env.DWP_INTEGRATION === '1' ? test : test.skip;

integration('the load runner measures three APIs and two real publishers using the normal defaults', async () => {
  const reportPath = join(tmpdir(), `dwp-load-smoke-${crypto.randomUUID()}.json`);
  const child = Bun.spawn([process.execPath, 'scripts/load.ts'], {
    env: { ...process.env, AUTH_ENABLED: 'false', OUTBOX_BATCH_SIZE: '10', LOAD_WARMUP_SECONDS: '0',
      LOAD_DURATION_SECONDS: '1', LOAD_OUTBOX_DRAIN_SECONDS: '60', LOAD_REPORT_PATH: reportPath },
    stdout: 'pipe', stderr: 'pipe',
  });
  const stdout = new Response(child.stdout).text();
  const stderr = new Response(child.stderr).text();
  const timer = setTimeout(() => child.kill('SIGTERM'), 60_000);
  const killTimer = setTimeout(() => child.kill('SIGKILL'), 77_000);
  try {
    expect(await child.exited, `${await stdout}\n${await stderr}`).toBe(0);
    const report = await Bun.file(reportPath).json();
    expect(report.complete).toBe(true);
    expect(report.configuration.publisherConfiguration).toEqual({ batchSize: 10, leaseMs: 30_000, idleMs: 250, errorMs: 1_000 });
    expect(report.configuration.apiProcessIds).toHaveLength(3);
    expect(report.configuration.publisherProcessIds).toHaveLength(2);
    expect(new Set([...report.configuration.apiProcessIds, ...report.configuration.publisherProcessIds]).size).toBe(5);
    expect(report.publisherProcessesSurvived).toBe(true);
    expect(report.outbox.publisherExitCodes).toEqual([0, 0]);
    expect(report.outbox.publisherMetrics).toHaveLength(2);
    const observedPublished = report.outbox.publisherMetrics.reduce((sum: number, process: { published: number }) => sum + process.published, 0);
    expect(observedPublished).toBe(Number(report.outbox.published));
    expect(report.outbox.remaining).toBe(0);
    expect(report.outbox.drainedWithinBudget).toBe(true);
    expect(report.outbox.remainingAtDrainDeadline).toBe(0);
    expect(report.reconciliation.consistent).toBe(true);
    expect(report.measurements).toHaveLength(6);
    for (const measurement of report.measurements) {
      expect(measurement.errors).toBe(0);
      expect(measurement.outbox.publisherRetries).toBe(0);
      expect(measurement.outbox.publisherFailures).toBe(0);
    }
  } finally {
    clearTimeout(timer);
    clearTimeout(killTimer);
    if (child.exitCode === null) child.kill('SIGKILL');
    await child.exited;
    await Promise.allSettled([stdout, stderr]);
    await rm(reportPath, { force: true });
  }
}, 80_000);
