export type ApplicationErrorKind = 'validation' | 'not_found' | 'conflict' | 'unavailable' | 'internal';

export class ApplicationError extends Error {
  constructor(
    public readonly kind: ApplicationErrorKind,
    public readonly code: string,
    message: string,
    cause?: unknown,
  ) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'ApplicationError';
  }
}
