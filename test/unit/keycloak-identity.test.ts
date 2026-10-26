import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { exportJWK, generateKeyPair, SignJWT, type JWK } from 'jose';
import { ApplicationError } from '../../dist/application/application-error.js';
import { KeycloakIdentity } from '../../dist/auth/keycloak-identity.js';

type Mode = 'available' | 'unavailable' | 'invalid-json' | 'invalid-shape' | 'invalid-key' | 'invalid-rsa-material' | 'mixed-keys' | 'timeout';

describe('OIDC verification and JWKS availability', () => {
  let server: ReturnType<typeof Bun.serve>;
  let privateKey: CryptoKey;
  let publicJwk: JWK;
  let issuer: string;
  let mode: Mode = 'available';
  let requests = 0;
  const slowResponses = new Set<ReturnType<typeof setTimeout>>();

  beforeAll(async () => {
    const pair = await generateKeyPair('RS256');
    privateKey = pair.privateKey;
    publicJwk = { ...await exportJWK(pair.publicKey), kid: 'current-key', alg: 'RS256', use: 'sig' };
    server = Bun.serve({
      port: 0, hostname: '127.0.0.1',
      fetch() {
        requests += 1;
        switch (mode) {
          case 'unavailable': return new Response('temporarily unavailable', { status: 503 });
          case 'invalid-json': return new Response('{');
          case 'invalid-shape': return Response.json({ keys: [null] });
          case 'invalid-key': return Response.json({ keys: [{ ...publicJwk, n: 17, e: true }] });
          case 'invalid-rsa-material': return Response.json({ keys: [{ ...publicJwk, n: 'AA', e: 'AQAB' }] });
          case 'mixed-keys': return Response.json({ keys: [
            { ...publicJwk, kid: 'previous-key', alg: 'RS512' },
            { ...publicJwk, alg: undefined },
          ] });
          case 'timeout': return new Promise<Response>(resolve => {
            const timer = setTimeout(() => {
              slowResponses.delete(timer);
              resolve(Response.json({ keys: [publicJwk] }));
            }, 3_500);
            slowResponses.add(timer);
          });
          default: return Response.json({ keys: [publicJwk] });
        }
      },
    });
    issuer = `http://127.0.0.1:${server.port}/review-realm`;
  });

  afterAll(async () => {
    await server?.stop(true);
    for (const timer of slowResponses) clearTimeout(timer);
  });

  function verifier(jwksUrl = `http://127.0.0.1:${server.port}/jwks`) {
    return new KeycloakIdentity({ issuer, audience: 'dwp-api', jwksUrl });
  }

  function token(options: { issuer?: string; audience?: string; expired?: boolean; kid?: string } = {}) {
    return new SignJWT({
      azp: 'provider-a', preferred_username: 'service-account-provider-a', typ: 'Bearer',
      realm_access: { roles: ['dwp-provider'] }, providerId: 'provider-a',
    }).setProtectedHeader({ alg: 'RS256', kid: options.kid ?? 'current-key' })
      .setSubject('provider-service').setIssuer(options.issuer ?? issuer).setAudience(options.audience ?? 'dwp-api')
      .setIssuedAt().setExpirationTime(options.expired ? Math.floor(Date.now() / 1000) - 1 : '5m').sign(privateKey);
  }

  async function expectUnavailable(identity: KeycloakIdentity, accessToken: string) {
    try {
      await identity.authenticate(`Bearer ${accessToken}`);
      throw new Error('Expected identity provider unavailability');
    } catch (error) {
      expect(error).toBeInstanceOf(ApplicationError);
      expect(error).toMatchObject({ kind: 'unavailable', code: 'IDENTITY_PROVIDER_UNAVAILABLE' });
    }
  }

  test('uses cached public keys during a provider outage and accepts standard rotational metadata', async () => {
    mode = 'mixed-keys';
    requests = 0;
    const identity = verifier();
    const accessToken = await token();
    expect(await identity.authenticate(`Bearer ${accessToken}`)).toMatchObject({ role: 'provider', providerId: 'provider-a' });
    mode = 'unavailable';
    expect(await identity.authenticate(`Bearer ${accessToken}`)).toMatchObject({ role: 'provider', providerId: 'provider-a' });
    expect(requests).toBe(1);
    await expectUnavailable(verifier(), accessToken);
  });

  test('classifies malformed JSON, JWKS shape and RSA material as provider failures', async () => {
    const accessToken = await token();
    for (const failureMode of ['invalid-json', 'invalid-shape', 'invalid-key', 'invalid-rsa-material'] as const) {
      mode = failureMode;
      await expectUnavailable(verifier(), accessToken);
    }
  });

  test('classifies connection failures as provider unavailability', async () => {
    const closed = Bun.serve({ port: 0, hostname: '127.0.0.1', fetch: () => Response.json({ keys: [publicJwk] }) });
    const url = `http://127.0.0.1:${closed.port}/jwks`;
    await closed.stop(true);
    await expectUnavailable(verifier(url), await token());
  });

  test('bounds JWKS retrieval and classifies timeout as provider unavailability', async () => {
    mode = 'timeout';
    await expectUnavailable(verifier(), await token());
  }, 10_000);

  test('keeps invalid signatures, claims and unknown signing keys unauthorized', async () => {
    mode = 'available';
    const accessToken = await token();
    const [header, payload, signature] = accessToken.split('.');
    const tampered = `${header}.${payload}.${signature![0] === 'A' ? 'B' : 'A'}${signature!.slice(1)}`;
    for (const invalid of [
      tampered,
      await token({ audience: 'another-api' }),
      await token({ issuer: `${issuer}/another` }),
      await token({ expired: true }),
      await token({ kid: 'unknown-key' }),
      'not-a-token',
    ]) {
      await expect(verifier().authenticate(`Bearer ${invalid}`)).rejects.toMatchObject({ status: 401 });
    }
  });
});
