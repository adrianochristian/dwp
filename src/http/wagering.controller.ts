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

interface StatusResponse {
  status(code: number): unknown;
}

@Controller()
export class WageringController {
  constructor(@Inject(WageringService) private readonly service: WageringService) {}

  @Post('wallets')
  @HttpCode(201)
  createWallet(@Body() body: unknown) {
    return this.service.createWallet(body);
  }

  @Get('wallets/:walletId')
  getWallet(@Param('walletId') walletId: string) {
    return this.service.getWallet(walletId);
  }

  @Post('wagering/transactions')
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
