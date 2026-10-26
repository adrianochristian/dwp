import { ArgumentsHost, Catch, HttpException, type ExceptionFilter } from '@nestjs/common';
import { ApplicationError, type ApplicationErrorKind } from '../application/application-error.js';
import { DomainError } from '../domain/domain-error.js';
import { errorChain, isInfrastructureFailure } from '../infrastructure/failure-classification.js';

interface ErrorBody {
  code: string;
  message: string;
}

interface JsonResponse {
  status(code: number): JsonResponse;
  json(body: ErrorBody): unknown;
}

const httpErrorCodes: Readonly<Record<number, string>> = Object.freeze({
  400: 'INVALID_REQUEST',
  401: 'UNAUTHORIZED',
  403: 'FORBIDDEN',
  404: 'NOT_FOUND',
  405: 'METHOD_NOT_ALLOWED',
  409: 'CONFLICT',
  413: 'PAYLOAD_TOO_LARGE',
  415: 'UNSUPPORTED_MEDIA_TYPE',
  422: 'BUSINESS_RULE_VIOLATION',
  429: 'RATE_LIMITED',
  500: 'INTERNAL_ERROR',
  503: 'INFRASTRUCTURE_UNAVAILABLE',
});

const applicationStatus: Readonly<Record<ApplicationErrorKind, number>> = Object.freeze({
  validation: 400,
  not_found: 404,
  conflict: 409,
  unavailable: 503,
  internal: 500,
});

const businessErrors = new Set([
  'INSUFFICIENT_FUNDS', 'REVERSAL_INSUFFICIENT_FUNDS', 'CURRENCY_MISMATCH',
  'WALLET_PLAYER_MISMATCH', 'MONEY_OVERFLOW', 'WALLET_VERSION_OVERFLOW',
  'REFERENCE_MISMATCH', 'REFERENCE_REJECTED', 'REFERENCE_FAILED', 'INVALID_REFERENCE_KIND',
  'REFERENCE_AMOUNT_MISMATCH', 'REVERSAL_ALREADY_PROCESSED', 'REFERENCE_EXPIRED',
]);

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null
    ? value as Record<string, unknown>
    : undefined;
}

function databaseCode(chain: Record<string, unknown>[]): string | undefined {
  return chain.map(fields => fields.code)
    .find((code): code is string => typeof code === 'string' && /^[A-Z0-9]{5}$/.test(code));
}

function parserError(exception: unknown): { status: number; body: ErrorBody } | undefined {
  const fields = record(exception);
  switch (fields?.type) {
    case 'entity.too.large':
      return { status: 413, body: { code: 'PAYLOAD_TOO_LARGE', message: 'The request body exceeds the supported size' } };
    case 'charset.unsupported':
    case 'encoding.unsupported':
      return { status: 415, body: { code: 'UNSUPPORTED_MEDIA_TYPE', message: 'The request charset or content encoding is unsupported' } };
    case 'entity.parse.failed':
    case 'request.size.invalid':
      return { status: 400, body: { code: 'INVALID_REQUEST', message: 'The request body is malformed' } };
    default:
      return undefined;
  }
}

@Catch()
export class ApiErrorFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<JsonResponse>();
    const parser = parserError(exception);
    if (parser) {
      response.status(parser.status).json(parser.body);
      return;
    }
    if (exception instanceof ApplicationError) {
      response.status(applicationStatus[exception.kind]).json({ code: exception.code, message: exception.message });
      return;
    }
    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      if (status === 500) {
        response.status(500).json({ code: 'INTERNAL_ERROR', message: 'An unexpected error occurred' });
        return;
      }
      const detail = exception.getResponse();
      const fields = record(detail);
      const code = typeof fields?.code === 'string'
        ? fields.code
        : httpErrorCodes[status] ?? 'HTTP_ERROR';
      const message = typeof detail === 'string'
        ? detail
        : typeof fields?.message === 'string'
          ? fields.message
          : exception.message;
      response.status(status).json({ code, message });
      return;
    }
    if (exception instanceof DomainError && businessErrors.has(exception.code)) {
      response.status(422).json({ code: exception.code, message: exception.message });
      return;
    }
    const chain = errorChain(exception);
    const code = databaseCode(chain);
    if (code === '23505') {
      response.status(409).json({ code: 'CONFLICT', message: 'A unique business identity already exists' });
      return;
    }
    if (code === '22P02') {
      response.status(400).json({ code: 'INVALID_REQUEST', message: 'A request value has an invalid format' });
      return;
    }
    if (isInfrastructureFailure(chain)) {
      response.status(503).json({
        code: 'INFRASTRUCTURE_UNAVAILABLE',
        message: 'The operation is temporarily unavailable; retry with the same idempotency key',
      });
      return;
    }
    response.status(500).json({ code: 'INTERNAL_ERROR', message: 'An unexpected error occurred' });
  }
}
