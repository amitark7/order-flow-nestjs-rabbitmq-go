import { Module } from '@nestjs/common';
import { OrdersService } from './orders.service';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Order } from './order.entity';
import { OrdersController } from './orders.controller';
import { RabbitmqModule } from '../rabbitmq/rabbitmq.module';
import { RabbitmqService } from '../rabbitmq/rabbitmq.service';
import { OutboxEvent } from '../outbox/outbox-event.entity';
import { OutboxPublisherService } from '../outbox/outbox-publisher.service';

@Module({
  imports:[TypeOrmModule.forFeature([Order,OutboxEvent]),RabbitmqModule],
  providers: [OrdersService,RabbitmqService,OutboxPublisherService],
  controllers: [OrdersController]
})
export class OrdersModule {}
