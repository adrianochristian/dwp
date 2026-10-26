import { SetMetadata } from '@nestjs/common';

export const ACCESS_POLICY = 'dwp:access';

export interface AccessDefinition {
  policy: 'operator' | 'provider' | 'public';
  providerScope?: 'body' | 'path' | 'transaction';
}

export function Access(policy: AccessDefinition['policy'], providerScope?: AccessDefinition['providerScope']) {
  return SetMetadata(ACCESS_POLICY, { policy, ...(providerScope ? { providerScope } : {}) } satisfies AccessDefinition);
}
