import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq.service';

@Module({
    providers:[RabbitmqService],
    exports:[RabbitmqModule]
})
export class RabbitmqModule {}
