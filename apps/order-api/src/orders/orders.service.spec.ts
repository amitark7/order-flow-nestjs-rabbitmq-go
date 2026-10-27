
import { jest } from '@jest/globals';
import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { DataSource, EntityManager } from 'typeorm';
import { OrdersService } from './orders.service';
import { Order } from './order.entity';
import { OutboxEvent } from '../outbox/outbox-event.entity';

describe('OrdersService', () => {
  let service: OrdersService;

  const mockOrderRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
  };

  const mockDataSource = {
    transaction: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrdersService,
        {
          provide: getRepositoryToken(Order),
          useValue: mockOrderRepository,
        },
        {
          provide: DataSource,
          useValue: mockDataSource,
        },
      ],
    }).compile();

    service = module.get<OrdersService>(OrdersService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('createOrder', () => {
    it('should save the order and its outbox event in one transaction', async () => {
      const input = {
        customerEmail: 'customer@example.com',
        amount: 499,
      };

      const savedOrder = {
        id: 'order-123',
        ...input,
      };

      const manager = {
        create: jest.fn()
          .mockReturnValueOnce(input)
          .mockReturnValueOnce({}),
        save: jest.fn()
          .mockImplementationOnce(async () => savedOrder)
          .mockImplementationOnce(async () => ({} as OutboxEvent)),
      };

      mockDataSource.transaction.mockImplementation(
        async (...args: unknown[]) => {
          const callback = args[0] as (
            manager: EntityManager,
          ) => Promise<unknown>;
          return callback(manager as unknown as EntityManager);
        },
      );

      const result = await service.createOrder(input);

      expect(mockDataSource.transaction).toHaveBeenCalledTimes(1);

      expect(manager.create).toHaveBeenNthCalledWith(
        1,
        Order,
        input,
      );

      expect(manager.save).toHaveBeenNthCalledWith(
        1,
        Order,
        input,
      );

      expect(manager.create).toHaveBeenNthCalledWith(
        2,
        OutboxEvent,
        expect.objectContaining({
          aggregateId: savedOrder.id,
          eventType: 'order.process',
          payload: expect.objectContaining({
            eventType: 'order.process',
            version: 1,
            data: {
              id: savedOrder.id,
              customerEmail: input.customerEmail,
              amount: input.amount,
            },
          }),
        }),
      );

      expect(manager.save).toHaveBeenNthCalledWith(
        2,
        OutboxEvent,
        expect.any(Object),
      );

      expect(result).toEqual(savedOrder);
    });


    it('should reject when saving the outbox event fails', async () => {
      const input = {
        customerEmail: 'customer@example.com',
        amount: 499,
      };

      const savedOrder = {
        id: 'order-123',
        ...input,
      };

      const outboxError = new Error('Outbox insert failed');

      const manager = {
        create: jest.fn()
          .mockReturnValueOnce(input)
          .mockReturnValueOnce({}),
        save: jest.fn()
          .mockImplementationOnce(async () => savedOrder)
          .mockImplementationOnce(async () => {
            throw outboxError;
          }),
      };

      mockDataSource.transaction.mockImplementation(
        async (...args: unknown[]) => {
          const callback = args[0] as (
            manager: EntityManager,
          ) => Promise<unknown>;
          return callback(manager as unknown as EntityManager);
        },
      );

      await expect(service.createOrder(input)).rejects.toThrow(
        'Outbox insert failed',
      );

      expect(manager.save).toHaveBeenCalledTimes(2);
    });

  });
});
