import { Module } from '@nestjs/common';
import { createObserveModule } from '@nestjs/observe';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { TypeOrmModule } from '@nestjs/typeorm';
import { OrdersModule } from './orders/orders.module';
import { RabbitmqService } from './rabbitmq/rabbitmq.service';
import { RabbitmqModule } from './rabbitmq/rabbitmq.module';

export const { ObserveModule, ObserveInstrument } = createObserveModule();

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: 'localhost',
      port: 5432,
      username: 'orderflow',
      password: 'local_dev_password',
      database: 'orderflow',
      autoLoadEntities: true,
      synchronize: false,
    }),
    OrdersModule,
    RabbitmqModule,
  ],
  controllers: [AppController],
  providers: [AppService, RabbitmqService],
})
export class AppModule {}
