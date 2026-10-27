
import {
  Column,
  CreateDateColumn,
  Entity,
  PrimaryGeneratedColumn,
} from 'typeorm';

@Entity({ name: 'order_api_outbox_events' })
export class OutboxEvent {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ name: 'aggregate_id', type: 'uuid' })
  aggregateId: string;

  @Column({ name: 'event_type', type: 'varchar', length: 100 })
  eventType: string;

  @Column({ type: 'jsonb' })
  payload: Record<string, unknown>;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  createdAt: Date;

  @Column({
    name: 'published_at',
    type: 'timestamptz',
    nullable: true,
  })
  publishedAt: Date | null;

  @Column({ type: 'integer', default: 0 })
  attempts: number;

  @Column({ name: 'last_error', type: 'text', nullable: true })
  lastError: string | null;


    @Column({
        name: 'next_attempt_at',
        type: 'timestamptz',
        default: () => 'NOW()',
    })
    nextAttemptAt: Date;

    @Column({
        name: 'failed_at',
        type: 'timestamptz',
        nullable: true,
    })
    failedAt: Date | null;

}
