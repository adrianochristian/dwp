import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { INestApplication } from '@nestjs/common';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';
import { bootstrapQueues } from '../../dist/infrastructure/messaging/bootstrap-queues.js';
import { createSqsClient } from '../../dist/infrastructure/messaging/sqs.js';
import { createOperationalApplication } from '../../dist/observability/operational-server.js';

const integration = process.env.DWP_INTEGRATION === '1' ? describe : describe.skip;

integration('operational liveness, readiness and current outbox metrics', () => {
  const databaseName = `dwp_ops_${crypto.randomUUID().replaceAll('-', '')}`;
  let admin: MikroORM;
  let orm: MikroORM;
  let sqs: SQSClient;
  let app: INestApplication;
  let address: string;

  beforeAll(async () => {
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    sqs = createSqsClient();
    await bootstrapQueues(sqs);
    app = await createOperationalApplication(orm, sqs);
    await app.listen(0, '127.0.0.1');
    address = await app.getUrl();
  });

  afterAll(async () => {
    await app?.close();
    sqs?.destroy();
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`alter database ${databaseName} allow_connections true`);
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
  });

  test('probes real PostgreSQL and the command and event queues', async () => {
    expect((await fetch(`${address}/health/live`)).status).toBe(200);
    const ready = await fetch(`${address}/health/ready`);
    expect(ready.status).toBe(200);
    expect(await ready.json()).toEqual({ status: 'ok' });
  });

  test('scrapes the actual oldest unpublished event age without financial payloads', async () => {
    const eventId = crypto.randomUUID();
    const aggregateId = crypto.randomUUID();
    const payload = { eventId, data: { privateId: 'private-provider-identifier', money: { amount: '7654321.00', currency: 'BRL' } } };
    await orm.em.fork().execute(`insert into dwp.outbox_messages(id, aggregate_id, event_type, payload, occurred_at)
      values (?, ?, 'OperationalTest', ?::jsonb, clock_timestamp() - interval '30 seconds')`, [eventId, aggregateId, JSON.stringify(payload)]);
    try {
      const response = await fetch(`${address}/metrics`);
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('text/plain');
      const body = await response.text();
      expect(body).toContain('# TYPE dwp_transactions_total counter');
      expect(body).toContain('# TYPE dwp_processing_duration_seconds histogram');
      const lag = Number(/^dwp_outbox_lag_seconds ([0-9.]+)$/m.exec(body)?.[1]);
      expect(lag).toBeGreaterThanOrEqual(30);
      expect(lag).toBeLessThan(120);
      expect(body).not.toContain(eventId);
      expect(body).not.toContain(aggregateId);
      expect(body).not.toContain('private-provider-identifier');
      expect(body).not.toContain('7654321.00');
      await orm.em.fork().execute('update dwp.outbox_messages set published_at = now() where id = ?', [eventId]);
      const updated = await (await fetch(`${address}/metrics`)).text();
      expect(updated).toContain('dwp_outbox_lag_seconds 0\n');
    } finally {
      await orm.em.fork().execute('delete from dwp.outbox_messages where id = ?', [eventId]);
    }
  });

  test('reports PostgreSQL unavailability while liveness remains healthy and recovers', async () => {
    try {
      await admin.em.getConnection().execute(`alter database ${databaseName} allow_connections false`);
      await admin.em.getConnection().execute('select pg_terminate_backend(pid) from pg_stat_activity where datname = ?', [databaseName]);
      const ready = await fetch(`${address}/health/ready`);
      expect(ready.status).toBe(503);
      expect(await ready.json()).toMatchObject({ code: 'DEPENDENCIES_UNAVAILABLE' });
      expect((await fetch(`${address}/health/live`)).status).toBe(200);
    } finally {
      await admin.em.getConnection().execute(`alter database ${databaseName} allow_connections true`);
    }
    expect((await fetch(`${address}/health/ready`)).status).toBe(200);
  });

  test('reports an unreachable SQS endpoint and closing operational HTTP preserves shared PostgreSQL', async () => {
    const unavailable = new SQSClient({
      region: 'us-east-1', endpoint: 'http://127.0.0.1:1', credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
      maxAttempts: 1, requestHandler: { connectionTimeout: 300, requestTimeout: 500 },
    });
    const isolated = await createOperationalApplication(orm, unavailable);
    await isolated.listen(0, '127.0.0.1');
    try {
      const url = await isolated.getUrl();
      const ready = await fetch(`${url}/health/ready`);
      expect(ready.status).toBe(503);
      expect(await ready.json()).toMatchObject({ code: 'DEPENDENCIES_UNAVAILABLE' });
      expect((await fetch(`${url}/health/live`)).status).toBe(200);
      expect((await fetch(`${url}/metrics`)).status).toBe(200);
    } finally {
      await isolated.close();
      unavailable.destroy();
    }
    expect(await orm.em.fork().execute('select 1 as alive')).toEqual([{ alive: 1 }]);
  });
});
