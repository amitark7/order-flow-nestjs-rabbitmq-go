import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfirmChannel, ChannelModel, connect } from 'amqplib';

@Injectable()
export class RabbitmqService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitmqService.name);

  private connection: ChannelModel;
  private channel: ConfirmChannel;

  async onModuleInit() {
    const url =
      process.env.RABBITMQ_URL ??
      'amqp://orderflow:local_dev_password@localhost:5672';

    this.connection = await connect(url);
    this.channel = await this.connection.createConfirmChannel();

    await this.channel.assertQueue('order.process', {
      durable: true,
    });

    this.logger.log('Connected to RabbitMQ with publisher confirms');
  }

  private async reconnect(): Promise<void> {
    this.logger.warn('Reconnecting to RabbitMQ...');

    const url =
      process.env.RABBITMQ_URL ??
      'amqp://orderflow:local_dev_password@localhost:5672';

    const connection = await connect(url);
    const channel = await connection.createConfirmChannel();

    await channel.assertQueue('order.process', {
      durable: true,
    });

    this.connection = connection;
    this.channel = channel;

    this.logger.log('RabbitMQ reconnection successful');
  }

  async publishOrder(order: {
    id: string;
    customerEmail: string;
    amount: number;
  }) {
    if (!this.channel) {
      throw new Error('RabbitMQ channel is not initialized');
    }

    const correlationId = crypto.randomUUID();

    const message = Buffer.from(
      JSON.stringify({
        eventId: crypto.randomUUID(),
        eventType: 'order.process',
        version: 1,
        occurredAt: new Date().toISOString(),
        data: order,
      }),
    );

    await new Promise<void>((resolve, reject) => {
      this.channel.sendToQueue(
        'order.process',
        message,
        {
          persistent: true,
          contentType: 'application/json',
          correlationId,
        },
        (error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        },
      );
    });

    this.logger.log(`RabbitMQ confirmed order ${order.id}`);
  }


async publishOutboxEvent(event: {
  id: string;
  payload: Record<string, unknown>;
}) {
  if (!this.channel) {
    await this.reconnect();
  }

  const message = Buffer.from(JSON.stringify(event.payload));

  try {
    await new Promise<void>((resolve, reject) => {
      this.channel.sendToQueue(
        'order.process',
        message,
        {
          persistent: true,
          contentType: 'application/json',
          messageId: event.id,
        },
        (error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        },
      );
    });
  } catch (error) {
    const errorMessage =
        error instanceof Error
        ? `${error.name}: ${error.message}`
        : String(error ?? 'Unknown publishing error');

    this.logger.error(
        `Publish failed for event ${event.id}: ${errorMessage}`,
    );

    await this.reconnect();

    // Retry once using the new channel.
    await new Promise<void>((resolve, reject) => {
      this.channel.sendToQueue(
        'order.process',
        message,
        {
          persistent: true,
          contentType: 'application/json',
          messageId: event.id,
        },
        (retryError) => {
          if (retryError) {
            reject(retryError);
          } else {
            resolve();
          }
        },
      );
    });
  }

  this.logger.log(`RabbitMQ confirmed outbox event ${event.id}`);
}


  async onModuleDestroy() {
    await this.channel?.close();
    await this.connection?.close();
  }
}
