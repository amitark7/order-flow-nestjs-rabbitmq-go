
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource } from 'typeorm';
import { randomUUID } from 'crypto';
import { Order } from './order.entity';
import { CreateOrderDto } from './dto/create-order.dto';
import { OutboxEvent } from '../outbox/outbox-event.entity';

@Injectable()
export class OrdersService {
  constructor(
    @InjectRepository(Order)
    private readonly orderRepository: Repository<Order>,
    private readonly dataSource: DataSource,
  ) {}

  async createOrder(data: CreateOrderDto) {
    const eventId = randomUUID();
    const occurredAt = new Date().toISOString();

    return this.dataSource.transaction(async (manager) => {
      const order = manager.create(Order, data);
      const savedOrder = await manager.save(Order, order);

      const eventPayload = {
        eventId,
        eventType: 'order.process',
        version: 1,
        occurredAt,
        data: {
          id: savedOrder.id,
          customerEmail: savedOrder.customerEmail,
          amount: savedOrder.amount,
        },
      };

      const outboxEvent = manager.create(OutboxEvent, {
        id: eventId,
        aggregateId: savedOrder.id,
        eventType: 'order.process',
        payload: eventPayload,
      });

      await manager.save(OutboxEvent, outboxEvent);

      return savedOrder;
    });
  }
}
