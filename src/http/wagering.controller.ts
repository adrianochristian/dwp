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

}
