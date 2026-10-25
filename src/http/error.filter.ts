import { ArgumentsHost, Catch, HttpException, type ExceptionFilter } from '@nestjs/common';
import { DomainError } from '../domain/domain-error.js';

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
  422: 'BUSINESS_RULE_VIOLATION',
  429: 'RATE_LIMITED',
  503: 'INFRASTRUCTURE_UNAVAILABLE',
});

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null
    ? value as Record<string, unknown>
    : undefined;
}

function databaseCode(error: unknown): string | undefined {
  let current = error;
  for (let depth = 0; depth < 4; depth += 1) {
    const fields = record(current);
    if (!fields) return undefined;
    if (typeof fields.code === 'string' && /^\d{5}$/.test(fields.code)) return fields.code;
    current = fields.cause;
  }
  return undefined;
}

@Catch()
export class ApiErrorFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<JsonResponse>();
    if (exception instanceof HttpException) {
      const status = exception.getStatus();
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
    if (exception instanceof DomainError) {
      response.status(422).json({ code: exception.code, message: exception.message });
      return;
    }
    const code = databaseCode(exception);
    if (code === '23505') {
      response.status(409).json({ code: 'CONFLICT', message: 'A unique business identity already exists' });
      return;
    }
    if (code === '22P02') {
      response.status(400).json({ code: 'INVALID_REQUEST', message: 'A request value has an invalid format' });
      return;
    }
    response.status(503).json({
      code: 'INFRASTRUCTURE_UNAVAILABLE',
      message: 'The operation is temporarily unavailable; retry with the same idempotency key',
    });
  }
}
