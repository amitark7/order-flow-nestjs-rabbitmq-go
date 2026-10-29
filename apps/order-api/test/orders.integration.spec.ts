
import { randomUUID } from 'node:crypto';
import {
  afterAll,
  beforeAll,
  describe,
  expect,
  it,
  jest,
} from '@jest/globals';
import { integrationDataSource } from './integration-data-source.ts';
import { OrdersService } from '../src/orders/orders.service.ts';
import { Order } from '../src/orders/order.entity.ts';
import { OutboxEvent } from '../src/outbox/outbox-event.entity.ts';

describe('OrdersService PostgreSQL integration', () => {
  let service: OrdersService;

  beforeAll(async () => {
    await integrationDataSource.initialize();

    // Use the real database and repositories.
    const orderRepository = integrationDataSource.getRepository(Order);

    service = new OrdersService(
      orderRepository,
      integrationDataSource,
    );
  });

  afterAll(async () => {
    if (integrationDataSource.isInitialized) {
      await integrationDataSource.destroy();
    }
  });

    it('persists an order and its outbox event in one transaction', async () => {
        const input = {
        customerEmail: `integration-${randomUUID()}@example.com`,
        amount: 499,
        };

        const order = await service.createOrder(input);

        expect(order.id).toBeDefined();
        expect(order.customerEmail).toBe(input.customerEmail);
        expect(order.amount).toBe(input.amount);
        expect(order.status).toBe('PENDING');

        const savedOrder = await integrationDataSource
        .getRepository(Order)
        .findOneByOrFail({ id: order.id });

        expect(savedOrder.customerEmail).toBe(input.customerEmail);

        const outboxEvent = await integrationDataSource
        .getRepository(OutboxEvent)
        .findOneBy({ aggregateId: order.id });

        expect(outboxEvent).not.toBeNull();
        expect(outboxEvent!.eventType).toBe('order.process');
        expect(outboxEvent!.payload).toMatchObject({
        eventType: 'order.process',
        version: 1,
        data: {
            id: order.id,
            customerEmail: input.customerEmail,
            amount: input.amount,
        },
        });
    });


    // it('rolls back the order when the outbox insert fails', async () => {
    // const input = {
    //     customerEmail: `rollback-${randomUUID()}@example.com`,
    //     amount: 499,
    // };

    // const originalSave = integrationDataSource.manager.save.bind(
    //     integrationDataSource.manager,
    // );

    // // Don't mock the transaction. We force the outbox save to fail
    // // inside the real transaction.
    // const failingManager = {
    //     create: integrationDataSource.manager.create.bind(
    //     integrationDataSource.manager,
    //     ),
    //     save: async (target: unknown, entity: unknown) => {
    //     if (target === OutboxEvent) {
    //         throw new Error('Simulated outbox insert failure');
    //     }

    //     return originalSave(target as never, entity as never);
    //     },
    // };

    // // We'll implement this test with a transaction-scoped manager
    // // in the next step, rather than replacing the global manager.
    // expect(failingManager).toBeDefined();
    // });

    it('rolls back the order when the outbox insert fails', async () => {
      const input = {
        customerEmail: `rollback-${randomUUID()}@example.com`,
        amount: 499,
      };

      const originalTransaction = integrationDataSource.transaction.bind(
        integrationDataSource,
      );

      const transactionSpy = jest
        .spyOn(integrationDataSource, 'transaction')
        .mockImplementation(async (runInTransaction: any) => {
          return originalTransaction(async (manager) => {
            const originalSave = manager.save.bind(manager);

            const failingManager = new Proxy(manager, {
              get(target, property) {
                if (property === 'save') {
                  return async (targetOrEntity: any, entity?: any) => {
                    if (targetOrEntity === OutboxEvent) {
                      throw new Error('Simulated outbox insert failure');
                    }

                    return originalSave(targetOrEntity, entity);
                  };
                }

                const value = Reflect.get(target, property, target);
                return typeof value === 'function'
                  ? value.bind(target)
                  : value;
              },
            });

            return runInTransaction(failingManager);
          });
        });

      try {
        await expect(service.createOrder(input)).rejects.toThrow(
          'Simulated outbox insert failure',
        );
      } finally {
        transactionSpy.mockRestore();
      }

      const persistedOrder = await integrationDataSource
        .getRepository(Order)
        .findOneBy({ customerEmail: input.customerEmail });

      expect(persistedOrder).toBeNull();
    });
});
