import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Inject,
  Param,
  Post,
  Query,
  Res,
} from '@nestjs/common';
import { WageringService } from '../application/wagering-service.js';
import { WageringQueries } from '../application/wagering-queries.js';
import { Access } from '../auth/access.decorator.js';

interface StatusResponse {
  status(code: number): unknown;
}

@Controller()
@Access('operator')
export class WageringController {
  constructor(
    @Inject(WageringService) private readonly service: WageringService,
    @Inject(WageringQueries) private readonly queries: WageringQueries,
  ) {}

  @Post('wallets')
  @HttpCode(201)
  createWallet(@Body() body: unknown) {
    return this.service.createWallet(body);
  }

  @Get('wallets/:walletId')
  getWallet(@Param('walletId') walletId: string) {
    return this.service.getWallet(walletId);
  }

  @Get('wallets/:walletId/ledger')
  ledger(@Param('walletId') walletId: string, @Query('cursor') cursor: unknown, @Query('limit') limit: unknown) {
    return this.queries.ledger(walletId, cursor, limit);
  }

  @Post('wallets/:walletId/reconciliation')
  @HttpCode(200)
  reconcile(@Param('walletId') walletId: string) {
    return this.queries.reconcile(walletId);
  }

  @Get('wagering/transactions/:transactionId')
  @Access('provider', 'transaction')
  getTransaction(@Param('transactionId') transactionId: string) {
    return this.queries.getTransaction(transactionId);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  @Access('provider', 'path')
  getExternalTransaction(@Param('providerId') providerId: string, @Param('externalTransactionId') externalTransactionId: string) {
    return this.queries.getExternalTransaction(providerId, externalTransactionId);
  }

  @Post('wagering/transactions')
  @Access('provider', 'body')
  @HttpCode(200)
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') idempotencyKey: unknown,
    @Res({ passthrough: true }) response: StatusResponse,
  ) {
    const result = await this.service.submit(body, idempotencyKey);
    switch (result.status) {
      case 'PROCESSED':
        response.status(200);
        break;
      case 'PENDING':
      case 'PENDING_REFERENCE':
        response.status(202);
        break;
      case 'REJECTED':
        response.status(422);
        break;
      default:
        response.status(503);
    }
    return result;
  }

}
