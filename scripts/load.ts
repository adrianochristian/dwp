import { createWageringService } from '../dist/infrastructure/persistence/wagering-services.js';
import { CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import { readFile } from 'node:fs/promises';
import { availableParallelism, cpus, freemem, platform, release, totalmem } from 'node:os';
import { resolve } from 'node:path';
import type { SubmissionResult } from '../dist/application/wagering-service.js';
import { Money } from '../dist/domain/money.js';
import { publisherConfiguration, type PublisherConfiguration } from '../dist/infrastructure/messaging/publisher-loop.js';
import { createSqsClient, sqsCall } from '../dist/infrastructure/messaging/sqs.js';
import { createDatabase } from '../dist/infrastructure/persistence/database.js';

type Scenario = 'hot-wallet' | 'independent-wallets';
type Child = ReturnType<typeof Bun.spawn>;
interface Instance {
  child: Child; address: string; logs: Promise<void>; errorLogs: Promise<void>;
  configuration?: PublisherConfiguration;
}
interface PublisherMetrics { published: number; retries: number; leaseLosses: number; failures: number }
interface Lag { pending: number; oldestSeconds: number }
interface Wallet { id: string; playerId: string }

class LoadExperimentError extends Error {
  constructor(readonly code: string, message: string) { super(message); this.name = 'LoadExperimentError'; }
}

function seconds(name: string, fallback: number, minimum: number): number {
  const raw = process.env[name] ?? String(fallback);
  if (!/^\d+$/.test(raw)) throw new LoadExperimentError('INVALID_CONFIGURATION', `${name} must be an integer`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > 3_600) {
    throw new LoadExperimentError('INVALID_CONFIGURATION', `${name} must be between ${minimum} and 3600`);
  }
  return value;
}

function increment(counts: Record<string, number>, key: string): void { counts[key] = (counts[key] ?? 0) + 1; }
function progress(event: string, data: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ event, timestamp: new Date().toISOString(), ...data }));
}

async function optionalText(path: string): Promise<string | null> {
  try { return (await readFile(path, 'utf8')).trim(); } catch { return null; }
}

async function processRss(pid: number): Promise<number> {
  const status = await optionalText(`/proc/${pid}/status`);
  const match = status?.match(/^VmRSS:\s+(\d+)\s+kB$/m);
  return match ? Number(match[1]) * 1_024 : 0;
}

async function startInstance(databaseUrl: string, eventQueueUrl?: string): Promise<Instance> {
  const child = Bun.spawn([process.execPath, eventQueueUrl ? 'dist/worker.js' : 'dist/main.js'], {
    env: {
      ...process.env, DATABASE_URL: databaseUrl, PORT: '0', AUTH_ENABLED: 'false',
      ...(eventQueueUrl ? { WORKER_ROLE: 'publisher', WORKER_METRICS_PORT: '0', EVENT_QUEUE_URL: eventQueueUrl } : {}),
    },
    stdout: 'pipe', stderr: 'pipe',
  });
  let started = false;
  let configuration: PublisherConfiguration | undefined;
  let resolveAddress: (value: string) => void = () => undefined;
  let rejectAddress: (error: Error) => void = () => undefined;
  const address = new Promise<string>((resolveStartup, rejectStartup) => {
    resolveAddress = resolveStartup; rejectAddress = rejectStartup;
  });
  const logs = (async () => {
    const reader = child.stdout.getReader();
    const decoder = new TextDecoder();
    let pending = '';
    try {
      while (true) {
        const read = await reader.read();
        if (read.done) break;
        pending += decoder.decode(read.value, { stream: true });
        let newline: number;
        while ((newline = pending.indexOf('\n')) !== -1) {
          const line = pending.slice(0, newline);
          pending = pending.slice(newline + 1);
          if (started) continue;
          try {
            const record = JSON.parse(line) as {
              event?: string; address?: string; metricsAddress?: string; publisherConfiguration?: PublisherConfiguration;
            };
            const startupAddress = eventQueueUrl && record.event === 'worker_started'
              ? record.metricsAddress : !eventQueueUrl && record.event === 'server_started' ? record.address : undefined;
            if (startupAddress) {
              started = true;
              configuration = record.publisherConfiguration;
              resolveAddress(startupAddress.replace('0.0.0.0', '127.0.0.1'));
            }
          } catch { /* Drain other startup diagnostics without retaining request logs. */ }
        }
        if (pending.length > 65_536) pending = '';
      }
      if (!started) rejectAddress(new LoadExperimentError('PROCESS_START_FAILED', 'An API process exited before listening'));
    } catch {
      if (!started) rejectAddress(new LoadExperimentError('PROCESS_START_FAILED', 'Could not read API process startup'));
    } finally { reader.releaseLock(); }
  })();
  const errorLogs = (async () => {
    const reader = child.stderr.getReader();
    try { while (!(await reader.read()).done) { /* Keep the process stderr pipe drained. */ } }
    finally { reader.releaseLock(); }
  })().catch(() => undefined);
  const timer = setTimeout(() => {
    rejectAddress(new LoadExperimentError('PROCESS_START_TIMEOUT', 'An API process did not listen within 15 seconds'));
  }, 15_000);
  try {
    const startupAddress = await address;
    if (eventQueueUrl && JSON.stringify(configuration) !== JSON.stringify(publisherConfiguration())) {
      throw new LoadExperimentError('PUBLISHER_CONFIGURATION_MISMATCH', 'The publisher did not use the shared runtime configuration');
    }
    return { child, address: startupAddress, logs, errorLogs, ...(configuration ? { configuration } : {}) };
  }
  catch (error) { child.kill('SIGKILL'); await child.exited; await Promise.allSettled([logs, errorLogs]); throw error; }
  finally { clearTimeout(timer); }
}

async function stopInstance(instance: Instance): Promise<void> {
  if (instance.child.exitCode === null) instance.child.kill('SIGTERM');
  const timer = setTimeout(() => instance.child.kill('SIGKILL'), 16_000);
  try {
    await instance.child.exited;
    await Promise.allSettled([instance.logs, instance.errorLogs]);
  } finally { clearTimeout(timer); }
}

async function publisherMetrics(instances: readonly Instance[]): Promise<PublisherMetrics[]> {
  return Promise.all(instances.map(async instance => {
    const response = await fetch(`${instance.address}/metrics`, { signal: AbortSignal.timeout(5_000) });
    if (!response.ok) throw new LoadExperimentError('PUBLISHER_METRICS_UNAVAILABLE', 'A publisher metrics endpoint is unavailable');
    const body = await response.text();
    const counter = (name: string): number => {
      const lines = body.split('\n').filter(line => line.startsWith(`dwp_${name}_total{role="publisher"} `)
        || line.startsWith(`dwp_${name}_total `));
      if (lines.length !== 1) throw new LoadExperimentError('PUBLISHER_METRICS_UNAVAILABLE', 'A publisher counter is missing or ambiguous');
      const value = Number(lines[0]!.slice(lines[0]!.lastIndexOf(' ') + 1));
      if (!Number.isFinite(value) || value < 0) throw new LoadExperimentError('PUBLISHER_METRICS_UNAVAILABLE', 'A publisher counter is invalid');
      return value;
    };
    return { published: counter('outbox_published'), retries: counter('retries'),
      leaseLosses: counter('outbox_lease_losses'), failures: counter('outbox_failures') };
  }));
}

function sumPublisherMetrics(values: readonly PublisherMetrics[]): PublisherMetrics {
  return values.reduce((total, value) => ({ published: total.published + value.published, retries: total.retries + value.retries,
    leaseLosses: total.leaseLosses + value.leaseLosses, failures: total.failures + value.failures }),
  { published: 0, retries: 0, leaseLosses: 0, failures: 0 });
}

async function lockConflicts(instances: readonly Instance[]): Promise<number | null> {
  try {
    const counts = await Promise.all(instances.map(async instance => {
      const response = await fetch(`${instance.address}/metrics`, { signal: AbortSignal.timeout(5_000) });
      if (!response.ok) throw new Error('Metrics unavailable');
      const metrics = await response.text();
      const lines = metrics.split('\n').filter(line => /^dwp_lock_conflicts_total(?:\{|\s)/.test(line));
      if (lines.length === 0) throw new Error('Lock metric unavailable');
      return lines.reduce((sum, line) => sum + Number(line.slice(line.lastIndexOf(' ') + 1)), 0);
    }));
    const total = counts.reduce((sum, value) => sum + value, 0);
    return Number.isFinite(total) ? total : null;
  } catch { return null; }
}

async function outboxLag(orm: MikroORM): Promise<Lag> {
  const [row] = await orm.em.fork().execute<{ pending: number; oldest_seconds: string }[]>(`
    select count(*)::int as pending,
      coalesce(extract(epoch from (clock_timestamp() - min(occurred_at))), 0)::text as oldest_seconds
    from dwp.outbox_messages where published_at is null`);
  return { pending: row!.pending, oldestSeconds: Math.max(0, Number(row!.oldest_seconds)) };
}

async function deadlocks(orm: MikroORM): Promise<number> {
  const [row] = await orm.em.fork().execute<{ deadlocks: string }[]>(
    'select deadlocks::text from pg_stat_database where datname = current_database()',
  );
  return Number(row!.deadlocks);
}

interface TrafficResult {
  requests: number;
  successful: number;
  errors: number;
  unexpectedReplays: number;
  elapsedSeconds: number;
  throughputRps: number;
  attemptedRps: number;
  errorRate: number;
  httpStatuses: Record<string, number>;
  transactionStatuses: Record<string, number>;
  failureCodes: Record<string, number>;
  latencyMs: { p50: number; p95: number; p99: number; max: number; resolution: number };
}

interface TrafficCounts { requests: number; successful: number; errors: number; unexpectedReplays: number }

export function validateTraffic(measurements: readonly TrafficCounts[], warmup: TrafficCounts, persistedBets: string) {
  const measured = measurements.reduce((sum, value) => ({ requests: sum.requests + value.requests,
    successful: sum.successful + value.successful, errors: sum.errors + value.errors,
    unexpectedReplays: sum.unexpectedReplays + value.unexpectedReplays }),
  { requests: 0, successful: 0, errors: 0, unexpectedReplays: 0 });
  const expectedBets = warmup.successful + measured.successful;
  return { warmup, measured, expectedBets, persistedBets,
    consistent: measurements.length === 6 && measurements.every(value => value.requests > 0)
      && measured.errors === 0 && measured.unexpectedReplays === 0 && measured.successful === measured.requests
      && warmup.errors === 0 && warmup.unexpectedReplays === 0 && warmup.successful === warmup.requests
      && BigInt(persistedBets) === BigInt(expectedBets) };
}

async function traffic(
  duration: number,
  wallets: readonly Wallet[],
  concurrency: number,
  instances: readonly Instance[],
  nextExternalId: () => string,
  interrupted: () => boolean,
): Promise<TrafficResult> {
  const histogram = new Uint32Array(60_001);
  const httpStatuses: Record<string, number> = {};
  const transactionStatuses: Record<string, number> = {};
  const failureCodes: Record<string, number> = {};
  let requests = 0;
  let successful = 0;
  let unexpectedReplays = 0;
  let maximumLatency = 0;
  const startedAt = performance.now();
  const deadline = startedAt + duration * 1_000;
  await Promise.all(Array.from({ length: concurrency }, async (_, worker) => {
    const wallet = wallets[worker % wallets.length]!;
    const instance = instances[worker % instances.length]!;
    while (performance.now() < deadline && !interrupted()) {
      const externalTransactionId = nextExternalId();
      const startedRequestAt = performance.now();
      let passed = false;
      try {
        const response = await fetch(`${instance.address}/wagering/transactions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'Idempotency-Key': `load-provider:${externalTransactionId}` },
          body: JSON.stringify({
            providerId: 'load-provider', externalTransactionId, playerId: wallet.playerId,
            walletId: wallet.id, roundId: 'load-round', gameId: 'load-game', kind: 'BET',
            money: { amount: '0.01', currency: 'BRL' },
          }),
          signal: AbortSignal.timeout(30_000),
        });
        increment(httpStatuses, String(response.status));
        const result = await response.json() as Partial<SubmissionResult> & { code?: string };
        increment(transactionStatuses, result.status ?? 'UNKNOWN');
        if (result.failureCode || result.code) increment(failureCodes, result.failureCode ?? result.code!);
        if (result.idempotentReplay === true) unexpectedReplays += 1;
        passed = response.status === 200 && result.status === 'PROCESSED' && result.idempotentReplay === false;
      } catch (error) {
        increment(httpStatuses, 'NETWORK_ERROR');
        increment(failureCodes, error instanceof Error ? error.name : 'UnknownError');
      }
      const elapsed = performance.now() - startedRequestAt;
      maximumLatency = Math.max(maximumLatency, elapsed);
      const bucket = Math.min(histogram.length - 1, Math.ceil(elapsed));
      histogram[bucket] = (histogram[bucket] ?? 0) + 1;
      requests += 1;
      if (passed) successful += 1;
    }
  }));
  const elapsedSeconds = (performance.now() - startedAt) / 1_000;
  const percentile = (fraction: number): number => {
    if (requests === 0) return 0;
    const rank = Math.ceil(requests * fraction);
    let seen = 0;
    for (let bucket = 0; bucket < histogram.length; bucket += 1) {
      seen += histogram[bucket]!;
      if (seen >= rank) return bucket;
    }
    return histogram.length - 1;
  };
  return {
    requests, successful, errors: requests - successful, unexpectedReplays, elapsedSeconds,
    throughputRps: elapsedSeconds > 0 ? successful / elapsedSeconds : 0,
    attemptedRps: elapsedSeconds > 0 ? requests / elapsedSeconds : 0,
    errorRate: requests > 0 ? (requests - successful) / requests : 0,
    httpStatuses, transactionStatuses, failureCodes,
    latencyMs: { p50: percentile(0.5), p95: percentile(0.95), p99: percentile(0.99), max: maximumLatency, resolution: 1 },
  };
}

async function reconcile(orm: MikroORM, initialBalance: Money, expectedWallets: number) {
  const wallets = await orm.em.fork().execute<{
    id: string; balance_cents: string; reconstructed_cents: string; processed_bets: string; debit_entries: string;
  }[]>(`
    select w.id, w.balance_cents::text,
      coalesce((select sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end)
        from dwp.wallet_ledger_entries l where l.wallet_id = w.id), 0)::text as reconstructed_cents,
      (select count(*) from dwp.wager_transactions t where t.wallet_id = w.id and t.kind = 'BET' and t.status = 'PROCESSED')::text as processed_bets,
      (select count(*) from dwp.wallet_ledger_entries l where l.wallet_id = w.id and l.direction = 'DEBIT')::text as debit_entries
    from dwp.wallets w order by w.id`);
  const [identity] = await orm.em.fork().execute<{
    transactions: string; unique_keys: string; unique_external_ids: string; ledger_entries: string; unique_ledger_effects: string;
  }[]>(`
    select
      (select count(*) from dwp.wager_transactions where kind = 'BET')::text as transactions,
      (select count(distinct idempotency_key) from dwp.wager_transactions where kind = 'BET')::text as unique_keys,
      (select count(distinct (provider_id, external_transaction_id)) from dwp.wager_transactions where kind = 'BET')::text as unique_external_ids,
      (select count(*) from dwp.wallet_ledger_entries)::text as ledger_entries,
      (select count(distinct (wallet_id, transaction_id)) from dwp.wallet_ledger_entries)::text as unique_ledger_effects`);
  const inconsistent = wallets.filter(wallet =>
    wallet.balance_cents !== wallet.reconstructed_cents
    || BigInt(wallet.balance_cents) < 0n
    || BigInt(wallet.balance_cents) !== initialBalance.cents - BigInt(wallet.processed_bets)
    || wallet.processed_bets !== wallet.debit_entries,
  );
  const identitiesUnique = identity!.transactions === identity!.unique_keys
    && identity!.transactions === identity!.unique_external_ids
    && identity!.ledger_entries === identity!.unique_ledger_effects;
  return {
    consistent: wallets.length === expectedWallets && inconsistent.length === 0 && identitiesUnique,
    walletsChecked: wallets.length,
    inconsistentWallets: inconsistent.map(wallet => wallet.id),
    identitiesUnique,
    counts: identity!,
    negativeBalances: wallets.filter(wallet => BigInt(wallet.balance_cents) < 0n).length,
  };
}

async function main(): Promise<void> {
  const warmupSeconds = seconds('LOAD_WARMUP_SECONDS', 15, 0);
  const measurementSeconds = seconds('LOAD_DURATION_SECONDS', 60, 1);
  const drainSeconds = seconds('LOAD_OUTBOX_DRAIN_SECONDS', 60, 1);
  const runId = crypto.randomUUID();
  const databaseName = `dwp_load_${runId.replaceAll('-', '')}`;
  const queueName = `dwp-load-events-${runId}`;
  const reportPath = resolve(process.env.LOAD_REPORT_PATH ?? 'load-results.json');
  const initialBalance = Money.from({ amount: '1000000.00', currency: 'BRL' });
  const sqs = createSqsClient();
  const instances: Instance[] = [];
  const publishers: Instance[] = [];
  let interrupted = false;
  const interrupt = () => { interrupted = true; };
  process.on('SIGINT', interrupt);
  process.on('SIGTERM', interrupt);
  let admin: MikroORM | undefined;
  let orm: MikroORM | undefined;
  let queueUrl: string | undefined;
  let createdDatabase = false;
  let requestSequence = 0;
  let walletCount = 0;
  const measurements: Record<string, unknown>[] = [];
  const trafficMeasurements: TrafficCounts[] = [];
  const warmupTraffic: TrafficCounts = { requests: 0, successful: 0, errors: 0, unexpectedReplays: 0 };
  const startedAt = new Date().toISOString();
  try {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    createdDatabase = true;
    const databaseUrl = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    databaseUrl.pathname = `/${databaseName}`;
    orm = await createDatabase(databaseUrl.toString());
    await orm.migrator.up();
    queueUrl = (await sqsCall(signal => sqs.send(new CreateQueueCommand({ QueueName: queueName }), { abortSignal: signal }))).QueueUrl;
    if (!queueUrl) throw new LoadExperimentError('QUEUE_SETUP_FAILED', 'SQS did not return a queue URL');
    const startupResults = await Promise.allSettled(Array.from({ length: 3 }, async () => {
      const instance = await startInstance(databaseUrl.toString());
      instances.push(instance);
    }));
    const startupFailure = startupResults.find(result => result.status === 'rejected');
    if (startupFailure?.status === 'rejected') throw startupFailure.reason;
    const publisherStarts = await Promise.allSettled(Array.from({ length: 2 }, async () => {
      publishers.push(await startInstance(databaseUrl.toString(), queueUrl!));
    }));
    const publisherStartFailure = publisherStarts.find(result => result.status === 'rejected');
    if (publisherStartFailure?.status === 'rejected') throw publisherStartFailure.reason;
    await publisherMetrics(publishers);
    const service = createWageringService(orm);
    for (const scenario of ['hot-wallet', 'independent-wallets'] as const) {
      for (const concurrency of [1, 10, 50]) {
        if (interrupted) throw new LoadExperimentError('INTERRUPTED', 'The load experiment was interrupted');
        const wallets = await Promise.all(Array.from({ length: scenario === 'hot-wallet' ? 1 : concurrency }, () =>
          service.createWallet({ playerId: crypto.randomUUID(), initialBalance: initialBalance.toJSON() }),
        ));
        walletCount += wallets.length;
        progress('load_warmup_started', { scenario, concurrency, seconds: warmupSeconds });
        const warmup = await traffic(warmupSeconds, wallets, concurrency, instances, () => `${runId}:${++requestSequence}`, () => interrupted);
        warmupTraffic.requests += warmup.requests;
        warmupTraffic.successful += warmup.successful;
        warmupTraffic.errors += warmup.errors;
        warmupTraffic.unexpectedReplays += warmup.unexpectedReplays;
        const conflictsBefore = await lockConflicts(instances);
        const deadlocksBefore = await deadlocks(orm);
        const lagBefore = await outboxLag(orm);
        const publisherBefore = sumPublisherMetrics(await publisherMetrics(publishers));
        const cpuBefore = process.cpuUsage();
        let driverPeakRssBytes = process.memoryUsage().rss;
        let apiPeakRssBytes = 0;
        let publisherPeakRssBytes = 0;
        let maximumLagSeconds = lagBefore.oldestSeconds;
        let maximumPending = lagBefore.pending;
        let sampleFailures = 0;
        let stopSampling: () => void = () => undefined;
        const finishedSampling = new Promise<void>(resolveSampling => { stopSampling = resolveSampling; });
        let sampling = true;
        const sampler = (async () => {
          while (sampling) {
            try {
              const [lag, memory, publisherMemory] = await Promise.all([
                outboxLag(orm!), Promise.all(instances.map(instance => processRss(instance.child.pid))),
                Promise.all(publishers.map(instance => processRss(instance.child.pid))),
              ]);
              maximumLagSeconds = Math.max(maximumLagSeconds, lag.oldestSeconds);
              maximumPending = Math.max(maximumPending, lag.pending);
              driverPeakRssBytes = Math.max(driverPeakRssBytes, process.memoryUsage().rss);
              apiPeakRssBytes = Math.max(apiPeakRssBytes, memory.reduce((sum, value) => sum + value, 0));
              publisherPeakRssBytes = Math.max(publisherPeakRssBytes, publisherMemory.reduce((sum, value) => sum + value, 0));
            } catch { sampleFailures += 1; }
            await Promise.race([Bun.sleep(1_000), finishedSampling]);
          }
        })();
        progress('load_measurement_started', { scenario, concurrency, seconds: measurementSeconds });
        let result: TrafficResult;
        try {
          result = await traffic(measurementSeconds, wallets, concurrency, instances, () => `${runId}:${++requestSequence}`, () => interrupted);
        } finally { sampling = false; stopSampling(); await sampler; }
        trafficMeasurements.push(result);
        const cpu = process.cpuUsage(cpuBefore);
        const conflictsAfter = await lockConflicts(instances);
        const lagAfter = await outboxLag(orm);
        const publisherAfter = sumPublisherMetrics(await publisherMetrics(publishers));
        const measurement = {
          scenario, concurrency, configuredSeconds: measurementSeconds, ...result,
          lockConflicts: conflictsBefore === null || conflictsAfter === null ? null : conflictsAfter - conflictsBefore,
          postgresDeadlocks: await deadlocks(orm) - deadlocksBefore,
          outbox: {
            start: lagBefore, end: lagAfter,
            maxObservedLagSeconds: Math.max(maximumLagSeconds, lagAfter.oldestSeconds),
            maxObservedPending: Math.max(maximumPending, lagAfter.pending), sampleIntervalMs: 1_000, sampleFailures,
            published: publisherAfter.published - publisherBefore.published,
            publisherRetries: publisherAfter.retries - publisherBefore.retries,
            leaseLosses: publisherAfter.leaseLosses - publisherBefore.leaseLosses,
            publisherFailures: publisherAfter.failures - publisherBefore.failures,
          },
          resources: { driverCpuSeconds: (cpu.user + cpu.system) / 1_000_000, driverPeakRssBytes, apiPeakRssBytes, publisherPeakRssBytes },
        };
        measurements.push(measurement);
        progress('load_measurement_finished', measurement);
      }
    }
    progress('load_outbox_draining', { seconds: drainSeconds });
    const drainStarted = performance.now();
    let finalLag = await outboxLag(orm);
    while (finalLag.pending > 0 && performance.now() - drainStarted < drainSeconds * 1_000 && !interrupted) {
      await Bun.sleep(100);
      finalLag = await outboxLag(orm);
    }
    const remainingAtDrainDeadline = finalLag.pending;
    const drainedWithinBudget = remainingAtDrainDeadline === 0;
    const drainElapsedSeconds = (performance.now() - drainStarted) / 1_000;
    const perPublisherMetrics = await publisherMetrics(publishers);
    const publisherTotals = sumPublisherMetrics(perPublisherMetrics);
    const publisherProcessIds = publishers.map(instance => instance.child.pid);
    const publisherProcessesRunning = publishers.every(instance => instance.child.exitCode === null && instance.child.signalCode === null);
    await Promise.all(publishers.map(stopInstance));
    const publisherExitCodes = publishers.map(instance => instance.child.exitCode);
    publishers.length = 0;
    const apiProcessIds = instances.map(instance => instance.child.pid);
    const apiProcessesRunning = instances.every(instance => instance.child.exitCode === null && instance.child.signalCode === null);
    await Promise.all(instances.map(stopInstance));
    instances.length = 0;
    finalLag = await outboxLag(orm);
    const reconciliation = await reconcile(orm, initialBalance, walletCount);
    const trafficValidation = validateTraffic(trafficMeasurements, warmupTraffic, reconciliation.counts.transactions);
    const queueAttributes = await sqsCall(signal => sqs.send(new GetQueueAttributesCommand({
      QueueUrl: queueUrl, AttributeNames: ['ApproximateNumberOfMessages'],
    }), { abortSignal: signal }));
    const [outbox] = await orm.em.fork().execute<{ total: string; published: string }[]>(`
      select count(*)::text as total, count(*) filter(where published_at is not null)::text as published from dwp.outbox_messages`);
    const report = {
      runId, startedAt, completedAt: new Date().toISOString(),
      configuration: {
        warmupSeconds, measurementSeconds, concurrency: [1, 10, 50], scenarios: ['hot-wallet', 'independent-wallets'],
        apiProcesses: 3, apiProcessIds, publisherProcesses: 2, publisherProcessIds,
        publisherConfiguration: publisherConfiguration(), authEnabled: false, providerId: 'load-provider',
        bet: { amount: '0.01', currency: 'BRL' }, initialBalance: initialBalance.toJSON(),
        histogramResolutionMs: 1, latencyIncludesFailedRequests: true,
        throughputDefinition: 'successful requests / elapsed measurement seconds, including final in-flight completion',
        warmupMetricsDiscarded: true, setupExcludedFromMeasurements: true,
      },
      environment: {
        bun: Bun.version, nodeCompatibility: process.version, platform: platform(), release: release(), arch: process.arch,
        availableParallelism: availableParallelism(), logicalCpus: cpus().length, cpuModel: cpus()[0]?.model ?? 'unknown',
        totalMemoryBytes: totalmem(), freeMemoryBytesAtCompletion: freemem(),
        cgroupCpuMax: await optionalText('/sys/fs/cgroup/cpu.max'), cgroupMemoryMax: await optionalText('/sys/fs/cgroup/memory.max'),
      },
      measurements,
      outbox: {
        ...outbox!, remaining: finalLag.pending, oldestPendingSeconds: finalLag.oldestSeconds,
        drainSeconds: drainElapsedSeconds, configuredDrainSeconds: drainSeconds, remainingAtDrainDeadline, drainedWithinBudget,
        approximateQueuedEvents: queueAttributes.Attributes?.ApproximateNumberOfMessages ?? null,
        publisherRetries: publisherTotals.retries, leaseLosses: publisherTotals.leaseLosses, publisherFailures: publisherTotals.failures,
        publisherMetrics: perPublisherMetrics, publisherExitCodes,
      },
      reconciliation,
      traffic: trafficValidation,
      apiProcessesSurvived: apiProcessesRunning,
      publisherProcessesSurvived: publisherProcessesRunning,
      complete: !interrupted && apiProcessesRunning && publisherProcessesRunning && publisherExitCodes.every(code => code === 0)
        && reconciliation.consistent && trafficValidation.consistent && drainedWithinBudget && finalLag.pending === 0,
    };
    await Bun.write(reportPath, `${JSON.stringify(report, null, 2)}\n`);
    progress('load_report_written', { reportPath, complete: report.complete });
    if (!report.complete) throw new LoadExperimentError('VALIDATION_FAILED', 'Traffic, process, reconciliation or outbox validation failed; inspect the load report');
  } finally {
    await Promise.allSettled(publishers.map(stopInstance));
    await Promise.allSettled(instances.map(stopInstance));
    await orm?.close(true);
    if (queueUrl) await sqsCall(signal => sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl }), { abortSignal: signal })).catch(() => undefined);
    sqs.destroy();
    if (admin) {
      if (createdDatabase) await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
    process.off('SIGINT', interrupt);
    process.off('SIGTERM', interrupt);
  }
}

if (import.meta.main) {
  await main().catch((error: unknown) => {
    progress('load_failed', {
      code: error instanceof LoadExperimentError ? error.code : 'DEPENDENCY_FAILURE',
      message: error instanceof LoadExperimentError ? error.message : 'The load experiment failed while accessing a dependency',
    });
    process.exitCode = 1;
  });
}
