import { afterAll, beforeAll, describe, test } from 'bun:test';
import assert from 'node:assert/strict';
import fc from 'fast-check';
import type { MikroORM } from '@mikro-orm/postgresql';
import { WageringService } from '../../src/application/wagering-service.js';
import { createDatabase } from '../../src/infrastructure/persistence/database.js';
import {
  WageringOracle, decimal, expectedBusinessHash, expectedInboxHash,
  type OracleBody, type OracleKind, type OracleRequest, type OracleResult, type OracleState, type OracleTransaction,
} from '../model/wagering-oracle.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;
const maximum = 9223372036854775807n;
const seeds = [424242, 424243, 424244] as const;
type Action = 'new' | 'replay' | 'payload_conflict' | 'external_conflict' | 'redelivery' | 'inbox_conflict' | 'retry_pending' | 'deliver_reference' | 'fail';
interface Step {
  action: Action;
  kind: Exclude<OracleKind, 'OPENING'>;
  wallet: number;
  cents: number;
  referenceMode: number;
  selector: number;
  alignReference: boolean;
  inbox: boolean;
  provider: number;
  round: number;
  identityError: number;
}
interface ActualResult {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  idempotentReplay: boolean;
  failureCode?: string;
}
interface TransactionRow {
  id: string; idempotency_key: string; provider_id: string; external_transaction_id: string;
  wallet_id: string; player_id: string; round_id: string; game_id: string; kind: string;
  amount_cents: string; currency: string; status: string; failure_code: string | null;
  reference_external_transaction_id: string | null; reference_transaction_id: string | null;
  payload_hash: string; result: ActualResult | null;
}

const stepArbitrary: fc.Arbitrary<Step> = fc.record({
  action: fc.constantFrom<Action>('new', 'new', 'new', 'replay', 'payload_conflict', 'external_conflict', 'redelivery', 'inbox_conflict', 'retry_pending', 'deliver_reference', 'fail'),
  kind: fc.constantFrom<Step['kind']>('BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'),
  wallet: fc.integer({ min: 0, max: 2 }), cents: fc.integer({ min: 0, max: 20_000 }),
  referenceMode: fc.integer({ min: 0, max: 3 }), selector: fc.integer({ min: 0, max: 40 }),
  alignReference: fc.boolean(), inbox: fc.boolean(), provider: fc.integer({ min: 0, max: 1 }),
  round: fc.integer({ min: 0, max: 1 }), identityError: fc.integer({ min: 0, max: 11 }),
});

const guided = (patch: Partial<Step>): Step => ({
  action: 'new', kind: 'BET', wallet: 1, cents: 1_000, referenceMode: 0,
  selector: 0, alignReference: true, inbox: false, provider: 0, round: 0, identityError: 2, ...patch,
});

// Each sequence first establishes all five operation types and a pending inbox
// acceptance that later settles. fast-check then explores up to eighteen mixed
// commands and can shrink that suffix to a reproducible counterexample.
const mixedPrefix: Step[] = [
  guided({}),
  guided({ kind: 'WIN', cents: 2_000, referenceMode: 1 }),
  guided({ kind: 'LOSS', cents: 0, referenceMode: 1 }),
  guided({ kind: 'REFUND', referenceMode: 1 }),
  guided({ kind: 'ROLLBACK', referenceMode: 1 }),
  guided({ kind: 'ROLLBACK', cents: 2_000, referenceMode: 1, selector: 1 }),
  guided({ kind: 'ROLLBACK', cents: 1_000, referenceMode: 1, selector: 2 }),
  guided({ kind: 'REFUND', cents: 700, referenceMode: 3, inbox: true }),
  guided({ action: 'deliver_reference' }),
  guided({ action: 'retry_pending' }),
  guided({ action: 'redelivery' }),
  guided({ action: 'replay', selector: 1 }),
];

function transactionList(state: OracleState): OracleTransaction[] {
  return [...state.transactions.values()].filter(transaction => transaction.body.kind !== 'OPENING');
}

function select<T>(values: T[], index: number): T | undefined { return values.length ? values[index % values.length] : undefined; }

function buildRequest(step: Step, state: OracleState, namespace: string, index: number): OracleRequest {
  const transactions = transactionList(state);
  const boxes = [...state.inbox.values()];
  const chosen = select(transactions, step.selector);
  const key = `${namespace}:key:${index}`;
  const message = { consumerName: namespace, messageId: `${namespace}:message:${index}` };
  if (step.action === 'redelivery' || step.action === 'inbox_conflict') {
    const box = select(boxes, step.selector);
    if (box) {
      const replay = structuredClone(box.request);
      if (step.action === 'inbox_conflict') replay.body.roundId += ':changed';
      return replay;
    }
  }
  if (step.action === 'retry_pending') {
    const pending = select(transactions.filter(transaction => transaction.status === 'PENDING_REFERENCE'), step.selector);
    if (pending) return { body: structuredClone(pending.body), key: pending.key };
  }
  if (chosen && ['replay', 'payload_conflict', 'external_conflict'].includes(step.action)) {
    const body = structuredClone(chosen.body);
    if (step.action === 'payload_conflict') body.roundId += ':changed';
    return { body, key: step.action === 'external_conflict' ? key : chosen.key, ...(step.inbox ? { inbox: message } : {}) };
  }
  if (step.action === 'fail' && chosen && step.selector % 2 === 0) {
    return { body: structuredClone(chosen.body), key: chosen.key, failureCode: 'GENERATED_PERMANENT_FAILURE', ...(step.inbox ? { inbox: message } : {}) };
  }
  if (step.action === 'deliver_reference') {
    const pending = select(transactions.filter(transaction => transaction.status === 'PENDING_REFERENCE'
      && transaction.body.referenceExternalTransactionId !== undefined
      && !transactions.some(reference => reference.body.providerId === transaction.body.providerId
        && reference.body.externalTransactionId === transaction.body.referenceExternalTransactionId)), step.selector);
    if (pending) {
      const { referenceExternalTransactionId: reference, ...body } = structuredClone(pending.body);
      return { key, body: { ...body, kind: 'BET', externalTransactionId: reference!,
        money: { amount: decimal(pending.cents > 0n ? pending.cents : 1n), currency: 'BRL' } } };
    }
  }

  const wallets = [...state.wallets.values()];
  let wallet = wallets[step.wallet]!;
  const kind = step.action === 'deliver_reference' ? 'BET' : step.kind;
  let body: OracleBody = {
    providerId: `provider-${step.provider}`, externalTransactionId: `${namespace}:external:${index}`,
    walletId: wallet.id, playerId: wallet.playerId, roundId: `round-${step.round}`, gameId: 'generated-game', kind,
    money: { amount: decimal(BigInt(kind === 'LOSS' ? step.cents : Math.max(1, step.cents))), currency: 'BRL' },
  };
  if (kind !== 'BET' && (step.referenceMode !== 0 || ['REFUND', 'ROLLBACK'].includes(kind))) {
    const eligibleKinds = kind === 'ROLLBACK' ? ['BET', 'WIN', 'REFUND'] : ['BET'];
    const candidates = step.referenceMode === 1
      ? transactions.filter(transaction => eligibleKinds.includes(transaction.body.kind))
      : transactions;
    const reference = step.referenceMode === 3 ? undefined : select(candidates, step.selector);
    if (reference) {
      body.referenceExternalTransactionId = reference.body.externalTransactionId;
      if (step.alignReference) {
        wallet = state.wallets.get(reference.body.walletId)!;
        body = { ...body, walletId: wallet.id, playerId: wallet.playerId,
          providerId: reference.body.providerId, roundId: reference.body.roundId };
      }
    } else body.referenceExternalTransactionId = `${namespace}:missing:${index}`;
  }
  if (step.identityError === 0) body.playerId = `wrong-${wallet.playerId}`;
  else if (step.identityError === 1) body.money.currency = 'USD';
  return { body, key, ...(step.inbox ? { inbox: message } : {}),
    ...(step.action === 'fail' ? { failureCode: 'GENERATED_PERMANENT_FAILURE' } : {}) };
}

function conflictCode(error: unknown): string | undefined {
  if (error === null || typeof error !== 'object') return undefined;
  const fields = error as { kind?: string; code?: string; status?: number; response?: { code?: string } };
  if (fields.kind === 'conflict') return fields.code;
  return fields.status === 409 ? fields.response?.code : undefined;
}

function renderResult(result: OracleResult, bindings: Map<string, string>): ActualResult {
  const transactionId = bindings.get(result.transactionKey);
  assert.ok(transactionId, `No persisted ID for model transaction ${result.transactionKey}`);
  return { transactionId, status: result.status, balance: result.balance, idempotentReplay: result.idempotentReplay,
    ...(result.failureCode ? { failureCode: result.failureCode } : {}) };
}

function comparable(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(comparable);
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => [key, comparable(item)]));
  }
  return value;
}

function bag(values: unknown[]): string[] { return values.map(value => JSON.stringify(comparable(value))).sort(); }

integration('independent integer-ledger model over generated SQL sequences', () => {
  const databaseName = `dwp_model_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;
  let sequence = 0;
  const coverage = { sequences: 0, commands: 0, conflicts: 0, inboxSnapshotReplays: 0,
    processedKinds: new Set<string>(), statuses: new Set<string>(), generatedActions: new Set<string>() };

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.fork().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new WageringService(orm);
  });

  afterAll(async () => {
    await orm?.close(true);
    if (admin) {
      await admin.em.fork().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  async function assertStoredState(state: OracleState, namespace: string): Promise<Map<string, string>> {
    const walletIds = [...state.wallets.keys()];
    const actualWallets = await orm.em.fork().execute<{ id: string; player_id: string; currency: string; balance_cents: string; version: number }[]>(
      'select id, player_id, currency, balance_cents::text, version from dwp.wallets where id in (?, ?, ?)', walletIds);
    assert.equal(actualWallets.length, 3);
    for (const row of actualWallets) {
      const wallet = state.wallets.get(row.id)!;
      assert.deepEqual({ playerId: row.player_id, currency: row.currency, cents: row.balance_cents, version: row.version },
        { playerId: wallet.playerId, currency: 'BRL', cents: wallet.cents.toString(), version: wallet.version });
      assert.ok(BigInt(row.balance_cents) >= 0n);
    }
    const transactions = await orm.em.fork().execute<TransactionRow[]>(`select id, idempotency_key, provider_id,
      external_transaction_id, wallet_id, player_id, round_id, game_id, kind, amount_cents::text,
      currency, status, failure_code, reference_external_transaction_id, reference_transaction_id,
      payload_hash, result from dwp.wager_transactions where wallet_id in (?, ?, ?)`, walletIds);
    assert.equal(transactions.length, state.transactions.size, 'Unexpected or missing transaction rows');
    const bindings = new Map(transactions.map(row => [row.idempotency_key, row.id]));
    for (const row of transactions) {
      const expected = state.transactions.get(row.idempotency_key);
      assert.ok(expected, `Unexpected persisted key ${row.idempotency_key}`);
      assert.deepEqual({ provider: row.provider_id, external: row.external_transaction_id, wallet: row.wallet_id,
        player: row.player_id, round: row.round_id, game: row.game_id, kind: row.kind, cents: row.amount_cents,
        currency: row.currency, status: row.status, failure: row.failure_code,
        referenceExternal: row.reference_external_transaction_id, referenceId: row.reference_transaction_id },
      { provider: expected.body.providerId, external: expected.body.externalTransactionId, wallet: expected.body.walletId,
        player: expected.body.playerId, round: expected.body.roundId, game: expected.body.gameId, kind: expected.body.kind,
        cents: expected.cents.toString(), currency: expected.body.money.currency, status: expected.status,
        failure: expected.failureCode ?? null, referenceExternal: expected.body.referenceExternalTransactionId ?? null,
        referenceId: expected.referenceKey ? bindings.get(expected.referenceKey) : null });
      assert.ok(expected.result);
      assert.deepEqual(row.result, renderResult(expected.result, bindings), `Stored response for ${row.idempotency_key}`);
      if (row.kind !== 'OPENING') assert.equal(row.payload_hash, expectedBusinessHash(expected.body));
    }
    const ledger = await orm.em.fork().execute<{ wallet_id: string; transaction_id: string; direction: string;
      amount_cents: string; currency: string; balance_before_cents: string; balance_after_cents: string; wallet_version: number }[]>(
      `select wallet_id, transaction_id, direction, amount_cents::text, currency, balance_before_cents::text,
       balance_after_cents::text, wallet_version from dwp.wallet_ledger_entries where wallet_id in (?, ?, ?)`, walletIds);
    const actualLedger = ledger.map(row => ({ walletId: row.wallet_id, transactionId: row.transaction_id, direction: row.direction,
      cents: row.amount_cents, currency: row.currency, before: row.balance_before_cents, after: row.balance_after_cents, version: row.wallet_version }));
    const expectedLedger = state.ledger.map(entry => ({ walletId: entry.walletId, transactionId: bindings.get(entry.transactionKey), direction: entry.direction,
      cents: entry.cents.toString(), currency: 'BRL', before: entry.before.toString(), after: entry.after.toString(), version: entry.version }));
    assert.deepEqual(bag(actualLedger), bag(expectedLedger), 'Immutable ledger effects');
    for (const wallet of state.wallets.values()) {
      const reconstructed = ledger.filter(entry => entry.wallet_id === wallet.id).reduce((sum, entry) =>
        sum + (entry.direction === 'CREDIT' ? BigInt(entry.amount_cents) : -BigInt(entry.amount_cents)), 0n);
      assert.equal(reconstructed, wallet.cents, 'Ledger reconstruction must equal the independent model balance');
    }
    const events = await orm.em.fork().execute<{ id: string; aggregate_id: string; event_type: string;
      payload: { eventId: string; aggregateId: string; eventType: string; correlationId: string; version: number; data: Record<string, unknown> } }[]>(
      `select id, aggregate_id, event_type, payload from dwp.outbox_messages
       where payload->'data'->>'walletId' in (?, ?, ?)`, walletIds);
    assert.equal(new Set(events.map(event => event.id)).size, events.length);
    const actualEvents = events.map(event => {
      assert.equal(event.payload.eventId, event.id);
      assert.equal(event.payload.aggregateId, event.aggregate_id);
      assert.equal(event.payload.eventType, event.event_type);
      return { type: event.event_type, aggregateId: event.aggregate_id, correlationId: event.payload.correlationId,
        version: event.payload.version, data: event.payload.data };
    });
    const expectedEvents = state.events.map(event => ({ type: event.eventType,
      aggregateId: event.eventType === 'WalletBalanceChanged' ? event.walletId : bindings.get(event.transactionKey),
      correlationId: bindings.get(event.transactionKey), version: 1,
      data: { transactionId: bindings.get(event.transactionKey), ...event.data } }));
    assert.deepEqual(bag(actualEvents), bag(expectedEvents), 'Versioned event data and original financial snapshots');
    const messages = await orm.em.fork().execute<{ consumer_name: string; message_id: string; payload_hash: string;
      processed_at: string | null; result: ActualResult }[]>('select * from dwp.inbox_messages where consumer_name = ?', [namespace]);
    assert.equal(messages.length, state.inbox.size, 'Inbox acceptance count');
    for (const message of messages) {
      const expected = [...state.inbox.values()].find(box => box.identity.consumerName === message.consumer_name && box.identity.messageId === message.message_id);
      assert.ok(expected);
      assert.ok(message.processed_at);
      assert.equal(message.payload_hash, expectedInboxHash(expected.request.body, expected.request.key));
      assert.deepEqual(message.result, renderResult(expected.result, bindings), 'Inbox keeps its original acceptance snapshot');
    }
    return bindings;
  }

  for (const seed of seeds) test(`seed ${seed}: thirty sequences, three wallets and at most thirty commands`, async () => {
    await fc.assert(fc.asyncProperty(fc.array(stepArbitrary, { minLength: 0, maxLength: 18 }), async generated => {
      const namespace = `model:${seed}:${++sequence}`;
      const initial = [0n, 10_000n, maximum - 100n];
      const fixtures = [];
      for (const [index, amount] of initial.entries()) {
        const wallet = await service.createWallet({ playerId: `${namespace}:player:${index}`, initialBalance: { amount: decimal(amount), currency: 'BRL' } });
        fixtures.push({ id: wallet.id, playerId: wallet.playerId, cents: amount });
      }
      const oracle = new WageringOracle(fixtures);
      await assertStoredState(oracle.snapshot(), namespace);
      const history: OracleRequest[] = [];
      const steps = [...mixedPrefix, ...generated];
      assert.ok(steps.length <= 30);
      for (const [index, step] of steps.entries()) {
        const request = buildRequest(step, oracle.snapshot(), namespace, index);
        history.push(structuredClone(request));
        const expected = oracle.submit(request);
        try {
          let actual: ActualResult | undefined;
          let error: unknown;
          try {
            actual = request.failureCode
              ? await service.failSubmission(request.body, request.key, request.failureCode, request.inbox)
              : await service.submit({ ...request.body, transport: 'ignored-model-metadata' }, request.key, request.inbox);
          } catch (caught) { error = caught; }
          if (expected.type === 'conflict') {
            assert.equal(conflictCode(error), expected.code, `Conflict outcome; actual error: ${String(error)}`);
            coverage.conflicts += 1;
          } else {
            if (error) throw error;
            assert.ok(actual);
            coverage.statuses.add(expected.result.status);
            if (expected.result.status === 'PROCESSED') coverage.processedKinds.add(request.body.kind);
            if (request.inbox && expected.result.status === 'PENDING_REFERENCE' && expected.result.idempotentReplay) coverage.inboxSnapshotReplays += 1;
          }
          const bindings = await assertStoredState(oracle.snapshot(), namespace);
          if (expected.type === 'result') assert.deepEqual(actual, renderResult(expected.result, bindings), 'Returned original result');
          coverage.commands += 1;
          coverage.generatedActions.add(step.action);
        } catch (error) {
          const snapshot = oracle.snapshot();
          throw new Error(`Model mismatch at seed ${seed}, sequence ${sequence}, step ${index}, descriptor ${JSON.stringify(step)}\n`
            + `History: ${JSON.stringify(history)}\nModel: ${JSON.stringify({ wallets: [...snapshot.wallets.values()],
              expected, transactions: snapshot.transactions.size, ledgerEntries: snapshot.ledger.length,
              events: snapshot.events.length, inboxAcceptances: snapshot.inbox.size }, (_, value) =>
              typeof value === 'bigint' ? value.toString() : value)}`, { cause: error });
        }
      }
      coverage.sequences += 1;
    }), { seed, numRuns: 30, verbose: true });
  }, 120_000);

  test('the fixed seeds exercised every operation, pending acceptance snapshots and conflict paths', () => {
    assert.equal(coverage.sequences, 90);
    assert.ok(coverage.commands >= 1080 && coverage.commands <= 2700);
    assert.deepEqual([...coverage.processedKinds].sort(), ['BET', 'LOSS', 'REFUND', 'ROLLBACK', 'WIN']);
    assert.ok(coverage.conflicts > 0);
    assert.ok(coverage.inboxSnapshotReplays >= 90);
    assert.ok(coverage.statuses.has('PENDING_REFERENCE'));
    assert.ok(coverage.statuses.has('REJECTED'));
    assert.ok(coverage.statuses.has('FAILED'));
    assert.deepEqual([...coverage.generatedActions].sort(),
      ['deliver_reference', 'external_conflict', 'fail', 'inbox_conflict', 'new', 'payload_conflict', 'redelivery', 'replay', 'retry_pending']);
    console.log(JSON.stringify({ event: 'model_sequences_verified', seeds, sequences: coverage.sequences,
      commands: coverage.commands, conflicts: coverage.conflicts, pendingInboxReplays: coverage.inboxSnapshotReplays }));
  });
});
