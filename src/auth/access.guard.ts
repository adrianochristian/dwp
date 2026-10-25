import { ForbiddenException, Inject, Injectable, type CanActivate, type ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { WageringQueries } from '../application/wagering-queries.js';
import { ACCESS_POLICY, type AccessDefinition } from './access.decorator.js';
import { PROVIDER_IDENTITY_PORT, type ProviderIdentity, type ProviderIdentityPort } from './provider-identity.js';

interface AuthenticatedRequest {
  method: string;
  headers: { authorization?: string | string[] };
  params: Record<string, string | undefined>;
  body?: unknown;
  identity?: ProviderIdentity;
}

function forbidden(): never {
  throw new ForbiddenException({ code: 'PROVIDER_ACCESS_DENIED', message: 'The client cannot access this resource' });
}

@Injectable()
export class AccessGuard implements CanActivate {
  constructor(
    @Inject(PROVIDER_IDENTITY_PORT) private readonly identities: ProviderIdentityPort,
    private readonly reflector: Reflector,
    @Inject(WageringQueries) private readonly queries: WageringQueries,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const access = this.reflector.getAllAndOverride<AccessDefinition>(ACCESS_POLICY, [context.getHandler(), context.getClass()])
      ?? { policy: 'operator' };
    if (access.policy === 'public' || !this.identities.enabled) return true;
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const authorization = request.headers.authorization;
    const identity = await this.identities.authenticate(typeof authorization === 'string' ? authorization : undefined);
    request.identity = identity;
    if (access.policy === 'operator') {
      if (identity.role !== 'operator') forbidden();
      return true;
    }
    if (identity.role === 'operator') {
      if (request.method === 'GET') return true;
      forbidden();
    }
    if (!identity.providerId) forbidden();
    switch (access.providerScope) {
      case 'body': {
        const body = request.body;
        if (body !== null && typeof body === 'object' && 'providerId' in body
          && typeof body.providerId === 'string' && body.providerId !== identity.providerId) forbidden();
        break;
      }
      case 'path':
        if (request.params.providerId !== identity.providerId) forbidden();
        break;
      case 'transaction': {
        if (!request.params.transactionId) forbidden();
        const transaction = await this.queries.getTransaction(request.params.transactionId);
        if (transaction.providerId !== identity.providerId) forbidden();
        break;
      }
    }
    return true;
  }
}
