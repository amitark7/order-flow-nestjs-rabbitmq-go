import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { Order } from '../src/orders/order.entity.ts';
import { OutboxEvent } from '../src/outbox/outbox-event.entity.ts';

const database = process.env.TEST_DATABASE;

if (database !== 'orderflow_test') {
  throw new Error(
    'Integration tests must use TEST_DATABASE=orderflow_test',
  );
}

export const integrationDataSource = new DataSource({
  type: 'postgres',
  host: process.env.TEST_DB_HOST ?? 'localhost',
  port: Number(process.env.TEST_DB_PORT ?? 5432),
  username: process.env.TEST_DB_USER ?? 'orderflow',
  password: process.env.TEST_DB_PASSWORD ?? 'local_dev_password',
  database,
  entities: [Order, OutboxEvent],
  synchronize: true,
  dropSchema: false,
});