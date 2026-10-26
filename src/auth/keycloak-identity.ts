import { ForbiddenException, UnauthorizedException } from '@nestjs/common';
import { createLocalJWKSet, createRemoteJWKSet, customFetch, errors, importJWK, jwtVerify, type FetchImplementation, type JSONWebKeySet, type JWTPayload } from 'jose';
import { ApplicationError } from '../application/application-error.js';
import type { ProviderIdentity, ProviderIdentityPort } from './provider-identity.js';

export interface IdentityConfiguration {
  issuer: string;
  audience: string;
  jwksUrl?: string;
}

function unauthorized(): never {
  throw new UnauthorizedException({ code: 'INVALID_ACCESS_TOKEN', message: 'A valid service access token is required' });
}

function identityProviderUnavailable(cause: unknown): ApplicationError {
  return new ApplicationError('unavailable', 'IDENTITY_PROVIDER_UNAVAILABLE',
    'The identity provider is temporarily unavailable', cause);
}

async function validateJwks(value: unknown): Promise<void> {
  // Validate provider-owned data at the fetch boundary. Token signature and
  // claim errors remain the verifier's responsibility and still produce 401.
  const resolver = createLocalJWKSet(value as JSONWebKeySet);
  for (const key of resolver.jwks().keys) {
    if (typeof key.kty !== 'string') throw new Error('The JWKS contains a malformed key');
    if (key.kty !== 'RSA' || (key.alg !== undefined && key.alg !== 'RS256')
      || (key.use !== undefined && key.use !== 'sig')
      || (key.key_ops !== undefined && !key.key_ops.includes('verify'))) continue;
    if (typeof key.n !== 'string' || typeof key.e !== 'string'
      || !/^[A-Za-z0-9_-]+$/.test(key.n) || !/^[A-Za-z0-9_-]+$/.test(key.e)) {
      throw new Error('The JWKS contains malformed RSA parameters');
    }
    const imported = await importJWK(key, 'RS256');
    if (imported instanceof Uint8Array || imported.type !== 'public') {
      throw new Error('The JWKS must provide public verification keys');
    }
    const algorithm = imported.algorithm as RsaHashedKeyAlgorithm;
    if (algorithm.modulusLength < 2048) {
      throw new Error('The JWKS does not meet the RS256 key size required by the verifier');
    }
  }
}

const fetchIdentityKeys: FetchImplementation = async (url, options) => {
  try {
    const response = await fetch(url, options);
    if (response.status !== 200) {
      await response.body?.cancel();
      throw new Error(`The JWKS endpoint returned HTTP ${response.status}`);
    }
    const document: unknown = await response.json();
    await validateJwks(document);
    return Response.json(document);
  } catch (error) {
    throw identityProviderUnavailable(error);
  }
};

export class KeycloakIdentity implements ProviderIdentityPort {
  readonly enabled = true;
  private readonly keys: ReturnType<typeof createRemoteJWKSet>;

  constructor(
    private readonly configuration: IdentityConfiguration,
    private readonly now: () => Date = () => new Date(),
  ) {
    const issuer = new URL(configuration.issuer);
    if (issuer.protocol !== 'https:' && issuer.protocol !== 'http:') throw new Error('OIDC issuer must use HTTP or HTTPS');
    this.keys = createRemoteJWKSet(new URL(configuration.jwksUrl ?? `${configuration.issuer}/protocol/openid-connect/certs`), {
      timeoutDuration: 3_000,
      cacheMaxAge: 300_000,
      cooldownDuration: 1_000,
      [customFetch]: fetchIdentityKeys,
    });
  }

  async authenticate(authorization: string | undefined): Promise<ProviderIdentity> {
    if (!authorization || !/^Bearer [A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(authorization)) unauthorized();
    let claims: JWTPayload;
    try {
      const verified = await jwtVerify(authorization.slice(7), this.keys, {
        algorithms: ['RS256'],
        issuer: this.configuration.issuer,
        audience: this.configuration.audience,
        currentDate: this.now(),
        requiredClaims: ['sub', 'iss', 'aud', 'exp', 'iat', 'azp'],
      });
      claims = verified.payload;
    } catch (error) {
      if (error instanceof ApplicationError) throw error;
      if (error instanceof errors.JOSEError) unauthorized();
      throw error;
    }
    if (typeof claims.sub !== 'string' || typeof claims.azp !== 'string'
      || claims.preferred_username !== `service-account-${claims.azp}` || claims.typ !== 'Bearer') unauthorized();
    const realmAccess = claims.realm_access;
    const roles = realmAccess && typeof realmAccess === 'object' && 'roles' in realmAccess && Array.isArray(realmAccess.roles)
      ? realmAccess.roles : [];
    if (roles.includes('dwp-operator')) return { subject: claims.sub, role: 'operator' };
    if (roles.includes('dwp-provider') && typeof claims.providerId === 'string' && claims.providerId.length > 0) {
      return { subject: claims.sub, role: 'provider', providerId: claims.providerId };
    }
    throw new ForbiddenException({ code: 'INSUFFICIENT_ROLE', message: 'The client has no application role' });
  }
}

class DisabledIdentity implements ProviderIdentityPort {
  readonly enabled = false;

  async authenticate(): Promise<ProviderIdentity> {
    return { subject: 'authentication-disabled', role: 'operator' };
  }
}

export function createIdentityPort(configuration?: IdentityConfiguration): ProviderIdentityPort {
  if (process.env.AUTH_ENABLED === 'false') return new DisabledIdentity();
  if (!configuration && (!process.env.OIDC_ISSUER || !process.env.OIDC_AUDIENCE)) {
    throw new Error('Authentication requires OIDC_ISSUER and OIDC_AUDIENCE; use AUTH_ENABLED=false explicitly for local testing');
  }
  return new KeycloakIdentity(configuration ?? {
    issuer: process.env.OIDC_ISSUER!,
    audience: process.env.OIDC_AUDIENCE!,
    ...(process.env.OIDC_JWKS_URL ? { jwksUrl: process.env.OIDC_JWKS_URL } : {}),
  });
}
