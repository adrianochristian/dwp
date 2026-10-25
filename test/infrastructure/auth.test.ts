import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { INestApplication } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import { decodeJwt } from 'jose';
import { createApplication } from '../../dist/application.js';
import { WageringService } from '../../dist/application/wagering-service.js';
import { KeycloakIdentity } from '../../dist/auth/keycloak-identity.js';
import { createDatabase } from '../../dist/infrastructure/persistence/database.js';

const integration = process.env.DWP_AUTH_INTEGRATION === '1' ? describe : describe.skip;

integration('Keycloak machine identities and provider authorization', () => {
  const databaseName = `dwp_auth_${crypto.randomUUID().replaceAll('-', '')}`;
  const issuer = process.env.OIDC_ISSUER ?? 'http://127.0.0.1:8080/realms/dwp';
  const audience = process.env.OIDC_AUDIENCE ?? 'dwp-api';
  let admin: MikroORM;
  let orm: MikroORM;
  let service: WageringService;
  let app: INestApplication;
  let address: string;
  let providerA: string;
  let providerB: string;
  let operator: string;
  let previousAuth: string | undefined;
  let previousIssuer: string | undefined;
  let previousAudience: string | undefined;

  async function token(clientId: string, variable: string): Promise<string> {
    const secret = process.env[variable];
    if (!secret) throw new Error(`Missing ${variable}; load the local authentication environment`);
    const response = await fetch(`${issuer}/protocol/openid-connect/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ grant_type: 'client_credentials', client_id: clientId, client_secret: secret }),
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) throw new Error(`Keycloak client_credentials failed for ${clientId}: HTTP ${response.status}`);
    const data = await response.json() as { access_token?: unknown };
    if (typeof data.access_token !== 'string') throw new Error('Keycloak returned no access token');
    return data.access_token;
  }

  beforeAll(async () => {
    [providerA, providerB, operator] = await Promise.all([
      token('provider-a', 'PROVIDER_A_CLIENT_SECRET'), token('provider-b', 'PROVIDER_B_CLIENT_SECRET'),
      token('operator', 'OPERATOR_CLIENT_SECRET'),
    ]);
    admin = await createDatabase();
    await admin.em.getConnection().execute(`create database ${databaseName}`);
    const url = new URL(process.env.DATABASE_URL ?? 'postgresql://dwp:dwp@127.0.0.1:5432/dwp');
    url.pathname = `/${databaseName}`;
    orm = await createDatabase(url.toString());
    await orm.migrator.up();
    service = new WageringService(orm);
    previousAuth = process.env.AUTH_ENABLED;
    previousIssuer = process.env.OIDC_ISSUER;
    previousAudience = process.env.OIDC_AUDIENCE;
    process.env.AUTH_ENABLED = 'true';
    process.env.OIDC_ISSUER = issuer;
    process.env.OIDC_AUDIENCE = audience;
    app = await createApplication(orm);
    await app.listen(0, '127.0.0.1');
    address = await app.getUrl();
  }, 30_000);

  afterAll(async () => {
    await app?.close();
    await orm?.close(true);
    if (admin) {
      await admin.em.getConnection().execute(`drop database if exists ${databaseName} with (force)`);
      await admin.close(true);
    }
    if (previousAuth === undefined) delete process.env.AUTH_ENABLED;
    else process.env.AUTH_ENABLED = previousAuth;
    if (previousIssuer === undefined) delete process.env.OIDC_ISSUER;
    else process.env.OIDC_ISSUER = previousIssuer;
    if (previousAudience === undefined) delete process.env.OIDC_AUDIENCE;
    else process.env.OIDC_AUDIENCE = previousAudience;
  });

  function headers(accessToken: string): Record<string, string> {
    return { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' };
  }

  async function fixture() {
    const playerId = crypto.randomUUID();
    const wallet = await service.createWallet({ playerId, initialBalance: { amount: '100.00', currency: 'BRL' } });
    const body = {
      providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), walletId: wallet.id, playerId,
      gameId: 'auth-game', roundId: 'auth-round', kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
    };
    return { wallet, body, key: `provider-a:${body.externalTransactionId}` };
  }

  async function assertConsistent(walletId: string): Promise<void> {
    const [row] = await orm.em.fork().execute<{ stored: string; calculated: string }[]>(`
      select w.balance_cents::text as stored,
        coalesce(sum(case when l.direction = 'CREDIT' then l.amount_cents::numeric else -l.amount_cents::numeric end), 0)::text as calculated
      from dwp.wallets w left join dwp.wallet_ledger_entries l on l.wallet_id = w.id
      where w.id = ? group by w.id`, [walletId]);
    expect(row).toBeDefined();
    expect(row!.stored).toBe(row!.calculated);
    expect(BigInt(row!.stored) >= 0n).toBe(true);
  }

  test('verifies real service tokens and rejects signature, issuer, audience and expired tokens', async () => {
    const identity = new KeycloakIdentity({ issuer, audience });
    expect(await identity.authenticate(`Bearer ${providerA}`)).toMatchObject({ role: 'provider', providerId: 'provider-a' });
    expect(await identity.authenticate(`Bearer ${operator}`)).toMatchObject({ role: 'operator' });
    const parts = providerA.split('.');
    const signature = parts[2]!;
    const tampered = `${parts[0]}.${parts[1]}.${signature[0] === 'A' ? 'B' : 'A'}${signature.slice(1)}`;
    await expect(identity.authenticate(`Bearer ${tampered}`)).rejects.toMatchObject({ status: 401 });
    await expect(new KeycloakIdentity({ issuer, audience: 'another-api' }).authenticate(`Bearer ${providerA}`))
      .rejects.toMatchObject({ status: 401 });
    await expect(new KeycloakIdentity({ issuer: `${issuer}/another-realm`, audience, jwksUrl: `${issuer}/protocol/openid-connect/certs` })
      .authenticate(`Bearer ${providerA}`)).rejects.toMatchObject({ status: 401 });
    const expiresAt = decodeJwt(providerA).exp!;
    await expect(new KeycloakIdentity({ issuer, audience }, () => new Date((expiresAt + 1) * 1_000))
      .authenticate(`Bearer ${providerA}`)).rejects.toMatchObject({ status: 401 });
  });

  test('keeps health public and rejects missing or malformed access tokens', async () => {
    expect((await fetch(`${address}/health/live`)).status).toBe(200);
    const { wallet } = await fixture();
    expect((await fetch(`${address}/wallets/${wallet.id}`)).status).toBe(401);
    expect((await fetch(`${address}/wallets/${wallet.id}`, { headers: { Authorization: 'Bearer malformed' } })).status).toBe(401);
    await assertConsistent(wallet.id);
  });

  test('allows a provider to submit and read its transactions but denies a different provider', async () => {
    const { wallet, body, key } = await fixture();
    const submitted = await fetch(`${address}/wagering/transactions`, {
      method: 'POST', headers: { ...headers(providerA), 'Idempotency-Key': key }, body: JSON.stringify(body),
    });
    expect(submitted.status).toBe(200);
    const result = await submitted.json() as { transactionId: string };
    expect((await fetch(`${address}/wagering/transactions/${result.transactionId}`, { headers: headers(providerA) })).status).toBe(200);
    expect((await fetch(`${address}/providers/provider-a/wagering/transactions/${body.externalTransactionId}`, { headers: headers(providerA) })).status).toBe(200);
    expect((await fetch(`${address}/wagering/transactions/${result.transactionId}`, { headers: headers(providerB) })).status).toBe(403);
    expect((await fetch(`${address}/providers/provider-a/wagering/transactions/${body.externalTransactionId}`, { headers: headers(providerB) })).status).toBe(403);
    expect((await fetch(`${address}/wagering/transactions`, {
      method: 'POST', headers: { ...headers(providerB), 'Idempotency-Key': `${key}:foreign` }, body: JSON.stringify(body),
    })).status).toBe(403);
    expect((await service.getWallet(wallet.id)).balance.amount).toBe('75.00');
    await assertConsistent(wallet.id);
  });

  test('reserves wallet administration for the operator and denies its provider-less submissions', async () => {
    const body = { playerId: crypto.randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } };
    expect((await fetch(`${address}/wallets`, { method: 'POST', headers: headers(providerA), body: JSON.stringify(body) })).status).toBe(403);
    const created = await fetch(`${address}/wallets`, { method: 'POST', headers: headers(operator), body: JSON.stringify(body) });
    expect(created.status).toBe(201);
    const wallet = await created.json() as { id: string };
    expect((await fetch(`${address}/wallets/${wallet.id}`, { headers: headers(operator) })).status).toBe(200);
    expect((await fetch(`${address}/wallets/${wallet.id}`, { headers: headers(providerA) })).status).toBe(403);
    expect((await fetch(`${address}/wallets/${wallet.id}/ledger`, { headers: headers(operator) })).status).toBe(200);
    expect((await fetch(`${address}/wallets/${wallet.id}/ledger`, { headers: headers(providerA) })).status).toBe(403);
    expect((await fetch(`${address}/wallets/${wallet.id}/reconciliation`, { method: 'POST', headers: headers(operator) })).status).toBe(200);
    expect((await fetch(`${address}/wallets/${wallet.id}/reconciliation`, { method: 'POST', headers: headers(providerA) })).status).toBe(403);
    const wager = { providerId: 'provider-a', externalTransactionId: crypto.randomUUID(), walletId: wallet.id, playerId: body.playerId,
      roundId: 'operator-round', gameId: 'operator-game', kind: 'BET', money: { amount: '25.00', currency: 'BRL' } };
    expect((await fetch(`${address}/wagering/transactions`, { method: 'POST', headers: { ...headers(operator), 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify(wager) })).status).toBe(403);
    await assertConsistent(wallet.id);
  });
});
