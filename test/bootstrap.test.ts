import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import { createApplication } from '../dist/application.js';

describe('application bootstrap', () => {
  let app: INestApplication;
  let address: string;

  beforeAll(async () => {
    app = await createApplication();
    await app.listen(0, '127.0.0.1');
    address = await app.getUrl();
  });

  afterAll(async () => {
    await app?.close();
  });

  test('serves public liveness over HTTP', async () => {
    const response = await fetch(`${address}/health/live`);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ status: 'ok' });
  });

  test('returns 404 for an unknown endpoint', async () => {
    const response = await fetch(`${address}/unknown`);
    expect(response.status).toBe(404);
  });
});
