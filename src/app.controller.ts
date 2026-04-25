import { Controller, Get, Post } from '@nestjs/common';
import { AppService } from './app.service';

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get('health')
  getHealth() {
    return this.appService.getHealth();
  }

  @Post('kafka/publish')
  async postTenMessages() {
    return this.appService.publishTenMessages();
  }

  @Post('kafka/consume-report')
  async consumeTopicAndReport() {
    return this.appService.consumeTopicAndReport();
  }
}
