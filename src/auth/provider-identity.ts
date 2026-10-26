export const PROVIDER_IDENTITY_PORT = Symbol('ProviderIdentityPort');

export interface ProviderIdentity {
  readonly subject: string;
  readonly role: 'operator' | 'provider';
  readonly providerId?: string;
}

export interface ProviderIdentityPort {
  readonly enabled: boolean;
  authenticate(authorization: string | undefined): Promise<ProviderIdentity>;
}
