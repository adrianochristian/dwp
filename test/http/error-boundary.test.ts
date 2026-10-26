import 'reflect-metadata';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Body, Controller, Get, HttpException, Module, Param, Post, type INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ApplicationError, type ApplicationErrorKind } from '../../dist/application/application-error.js';
import { DomainError } from '../../dist/domain/domain-error.js';
import { ApiErrorFilter } from '../../dist/http/error.filter.js';

@Controller('probe')
class ErrorProbeController {
  @Post('body')
  body(@Body() _body: unknown) {
    return { accepted: true };
  }

  @Get('application/:kind')
  application(@Param('kind') kind: ApplicationErrorKind): never {
    throw new ApplicationError(kind, 'EXPLICIT_APPLICATION_CODE', 'A safe application message', new Error('private cause'));
  }

  @Get('failure/:kind')
  failure(@Param('kind') kind: string): never {
    switch (kind) {
      case 'financial': throw new DomainError('CURRENCY_MISMATCH', 'The currency must match');
      case 'invariant': throw new DomainError('INVALID_TRANSACTION_STATE', 'private invariant details');
      case 'legacy-contract': throw new HttpException({ code: 'REFERENCE_REQUIRED', message: 'A reference is required' }, 400);
      case 'unique': throw Object.assign(new Error('private SQL text'), { code: '23505' });
      case 'invalid-database-value': throw Object.assign(new Error('private SQL text'), { code: '22P02' });
      case 'deadlock': throw new Error('private outer details', { cause: Object.assign(new Error('private deadlock details'), { code: '40P01' }) });
      case 'serialization': throw Object.assign(new Error('private SQL text'), { code: '40001' });
      case 'connection': throw Object.assign(new Error('private network details'), { code: 'ECONNREFUSED' });
      case 'sqs-timeout': throw Object.assign(new Error('private queue details'), { code: 'SQS_CALL_TIMEOUT' });
      case 'database-startup': throw new Error('private outer details', { cause: Object.assign(new Error('private database details'), {
        code: '55000', severity: 'FATAL', routine: 'CheckMyDatabase',
      }) });
      case 'database-startup-legacy': throw Object.assign(new Error('private database details'), {
        code: '55000', severity: 'FATAL', routine: 'InitPostgres',
      });
      case 'sql-prerequisite': throw Object.assign(new Error('private SQL text'), { code: '55000', severity: 'ERROR', routine: 'CheckMyDatabase' });
      case 'sql-prerequisite-unknown': throw Object.assign(new Error('private SQL text'), { code: '55000', severity: 'FATAL', routine: 'UnknownOperation' });
      case 'sql-syntax': throw Object.assign(new Error('private SQL text'), { code: '42601' });
      default: throw new TypeError('private programming details');
    }
  }
}

@Module({ controllers: [ErrorProbeController] })
class ErrorProbeModule {}

describe('HTTP error boundary', () => {
  let app: INestApplication;
  let address: string;

  beforeAll(async () => {
    app = await NestFactory.create(ErrorProbeModule, { logger: false });
    app.useGlobalFilters(new ApiErrorFilter());
    await app.listen(0, '127.0.0.1');
    address = await app.getUrl();
  });

  afterAll(async () => { await app?.close(); });

  function post(body: string, contentType = 'application/json', encoding?: string) {
    return fetch(`${address}/probe/body`, {
      method: 'POST', body,
      headers: { 'content-type': contentType, ...(encoding ? { 'content-encoding': encoding } : {}) },
    });
  }

  test('rejects malformed JSON and oversized bodies as permanent request errors', async () => {
    const invalid = await post('{"money":');
    expect(invalid.status).toBe(400);
    expect(await invalid.json()).toHaveProperty('code', 'INVALID_REQUEST');
    const oversized = await post(JSON.stringify({ padding: 'x'.repeat(110_000) }));
    expect(oversized.status).toBe(413);
    expect(await oversized.json()).toEqual({
      code: 'PAYLOAD_TOO_LARGE', message: 'The request body exceeds the supported size',
    });
    const valid = await post('{"money":{"amount":"1.00","currency":"BRL"}}');
    expect(valid.status).toBe(201);
    expect(await valid.json()).toEqual({ accepted: true });
  });

  test('rejects unsupported charsets and encodings without suggesting infrastructure retry', async () => {
    for (const [contentType, encoding] of [
      ['application/json; charset=iso-8859-1', undefined],
      ['application/json', 'compress'],
    ] as const) {
      const response = await post('{}', contentType, encoding);
      expect(response.status).toBe(415);
      expect(await response.json()).toEqual({
        code: 'UNSUPPORTED_MEDIA_TYPE', message: 'The request charset or content encoding is unsupported',
      });
    }
  });

  test('maps explicit application failures while keeping causes private', async () => {
    for (const [kind, status] of [
      ['validation', 400], ['not_found', 404], ['conflict', 409], ['unavailable', 503], ['internal', 500],
    ] as const) {
      const response = await fetch(`${address}/probe/application/${kind}`);
      expect(response.status).toBe(status);
      expect(await response.json()).toEqual(kind === 'internal'
        ? { code: 'INTERNAL_ERROR', message: 'An unexpected error occurred' }
        : { code: 'EXPLICIT_APPLICATION_CODE', message: 'A safe application message' });
    }
    const cause = new Error('original cause');
    expect(new ApplicationError('unavailable', 'EXPLICIT_APPLICATION_CODE', 'safe', cause).cause).toBe(cause);
  });

  test('preserves financial and contract codes and distinguishes operational SQL errors', async () => {
    for (const [kind, status, code] of [
      ['financial', 422, 'CURRENCY_MISMATCH'],
      ['legacy-contract', 400, 'REFERENCE_REQUIRED'],
      ['unique', 409, 'CONFLICT'],
      ['invalid-database-value', 400, 'INVALID_REQUEST'],
      ['deadlock', 503, 'INFRASTRUCTURE_UNAVAILABLE'],
      ['serialization', 503, 'INFRASTRUCTURE_UNAVAILABLE'],
      ['connection', 503, 'INFRASTRUCTURE_UNAVAILABLE'],
      ['sqs-timeout', 503, 'INFRASTRUCTURE_UNAVAILABLE'],
      ['database-startup', 503, 'INFRASTRUCTURE_UNAVAILABLE'],
      ['database-startup-legacy', 503, 'INFRASTRUCTURE_UNAVAILABLE'],
      ['sql-prerequisite-unknown', 503, 'INFRASTRUCTURE_UNAVAILABLE'],
    ] as const) {
      const response = await fetch(`${address}/probe/failure/${kind}`);
      expect(response.status).toBe(status);
      const body = await response.json();
      expect(body).toHaveProperty('code', code);
      expect(JSON.stringify(body)).not.toContain('private');
    }
  });

  test('returns internal errors for programming, invariant and SQL syntax failures', async () => {
    for (const kind of ['unknown', 'invariant', 'sql-syntax', 'sql-prerequisite']) {
      const response = await fetch(`${address}/probe/failure/${kind}`);
      expect(response.status).toBe(500);
      expect(await response.json()).toEqual({ code: 'INTERNAL_ERROR', message: 'An unexpected error occurred' });
    }
  });
});
