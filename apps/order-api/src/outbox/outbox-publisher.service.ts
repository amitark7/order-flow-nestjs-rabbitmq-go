
import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull, LessThanOrEqual } from 'typeorm';
import { OutboxEvent } from './outbox-event.entity';
import { RabbitmqService } from '../rabbitmq/rabbitmq.service';

@Injectable()
export class OutboxPublisherService


  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(OutboxPublisherService.name);
  private timer: ReturnType<typeof setInterval> | undefined;
  private isProcessing = false;

  constructor(
    @InjectRepository(OutboxEvent)
    private readonly outboxRepository: Repository<OutboxEvent>,
    private readonly rabbitmqService: RabbitmqService,
  ) {}
  private readonly MAX_ATTEMPTS = 5;

  onModuleInit() {
    this.timer = setInterval(() => {
      void this.publishPendingEvents();
    }, 2000);

    void this.publishPendingEvents();
  }

  async publishPendingEvents() {
    if (this.isProcessing) {
      return;
    }

    this.isProcessing = true;

    try {
        const events = await this.outboxRepository.find({
        where: {
            publishedAt: IsNull(),
            failedAt: IsNull(),
            nextAttemptAt: LessThanOrEqual(new Date()),
        },
        order: { createdAt: 'ASC' },
        take: 10,
        });

      for (const event of events) {
        try {
          await this.rabbitmqService.publishOutboxEvent({
            id: event.id,
            payload: event.payload,
          });

          event.publishedAt = new Date();
          event.attempts += 1;
          event.lastError = null;

          await this.outboxRepository.save(event);
        } catch (error: unknown) {
            const errorMessage =
                error instanceof Error
                    ? `${error.name}: ${error.message}`
                    : String(error ?? 'Unknown publishing error');

            this.logger.error(
                `Raw publish error: ${errorMessage}`,
            );

            event.attempts += 1;
            event.lastError = errorMessage;
            this.logger.error(
                `DEBUG outbox ${event.id}: errorMessage=${JSON.stringify(errorMessage)}, lastError=${JSON.stringify(event.lastError)}`,
            );

            if (event.attempts >= this.MAX_ATTEMPTS) {
                event.failedAt = new Date();

                await this.outboxRepository.save(event);

                this.logger.error(
                    `Outbox event ${event.id} permanently failed after ` +
                        `${event.attempts} attempts: ${event.lastError}`,
                );

                continue;
            }

            const delaySeconds = Math.min(
                2 * 2 ** (event.attempts - 1),
                60,
            );

            event.nextAttemptAt = new Date(
                Date.now() + delaySeconds * 1000,
            );

            await this.outboxRepository.save(event);

            this.logger.warn(
                `Outbox event ${event.id} failed on attempt ${event.attempts}; ` +
                    `retrying in ${delaySeconds}s: ${event.lastError}`,
            );
        }
      }
    } catch (error) {
      this.logger.error(
        `Outbox polling failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    } finally {
      this.isProcessing = false;
    }
  }

  onModuleDestroy() {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }
}
