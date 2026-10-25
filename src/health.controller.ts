import { Controller, Get } from '@nestjs/common';
import { Access } from './auth/access.decorator.js';

@Controller('health')
@Access('public')
export class HealthController {
  @Get('live')
  live(): { status: 'ok' } {
    return { status: 'ok' };
  }
}
